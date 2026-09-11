# ✈️ Toulouse Aviation Data Platform

End-to-end Data Engineering platform for ingesting, processing, validating and transforming aircraft position data.

The project demonstrates the design and industrialisation of a modern data pipeline combining **streaming ingestion, distributed processing, cloud storage, analytics, orchestration, Infrastructure as Code, Data Quality and CI/CD**.

> Portfolio Data Engineering project built around an aviation use case and a GCP-oriented architecture.

---

## 🎯 Project objective

Aircraft position data is continuously generated and must be ingested, stored, validated and transformed before it can be used for analytics.

The platform is designed to address the following data flow:

```text
Aircraft positions
        |
        v
Python Producer
        |
        v
      Kafka
        |
        v
Python Consumer
        |
        v
     GCS RAW
        |
        v
     PySpark
      /    \
     v      v
PROCESSED  REJECTED
     |
     v
  BigQuery
     |
     v
     dbt
     |
     v
Analytics / Power BI
```

**Apache Airflow** orchestrates batch processing and dependencies.

**Terraform** manages cloud infrastructure.

**Docker** provides reproducible runtime environments.

**GitHub Actions** validates the project through CI/CD controls.

---

## 🏗️ Architecture

The platform separates ingestion, storage, processing, transformation and orchestration responsibilities.

### Streaming ingestion

```text
Aviation source
      ↓
Python Kafka Producer
      ↓
Kafka topic
      ↓
Python Consumer
      ↓
GCS RAW
```

Kafka is used to decouple data producers from consumers and support scalable streaming ingestion.

Aircraft identifiers can be used as Kafka keys to preserve ordering for the same aircraft while distributing events across partitions.

### Batch processing

```text
GCS RAW
   ↓
PySpark
   ↓
Data Quality
   ↓
Deduplication
   ↓
Enrichment
   ↓
PROCESSED / REJECTED
```

PySpark performs distributed transformations, validation, deduplication and enrichment.

### Analytics layer

```text
Processed data
      ↓
BigQuery
      ↓
dbt
      ↓
Staging
      ↓
Intermediate
      ↓
Marts
      ↓
Analytics / BI
```

dbt manages SQL transformations, tests, documentation and lineage in the analytical layer.

---

## 🧰 Technology stack

| Area | Technologies |
|---|---|
| Language | Python, SQL |
| Streaming | Apache Kafka |
| Distributed processing | Apache Spark / PySpark |
| Cloud | Google Cloud Platform |
| Storage | Google Cloud Storage |
| Data Warehouse | BigQuery |
| Transformation | dbt |
| Orchestration | Apache Airflow |
| Infrastructure as Code | Terraform |
| Containers | Docker / Docker Compose |
| CI/CD | GitHub Actions |
| Data Quality | PySpark validation rules, automated tests, Quality Gates |
| Testing | pytest |
| Lakehouse experiments | Delta Lake |
| Analytics target | Power BI |

---

## 📡 Kafka streaming pipeline

The streaming layer implements:

- Kafka producer and consumer;
- partitioned topic;
- aircraft identifier as message key;
- consumer groups;
- offset management;
- controlled commits;
- producer idempotence;
- batching;
- Kafka-to-GCS ingestion.

The Kafka-to-GCS consumer commits offsets only after the corresponding data batch has been persisted.

This provides an **at-least-once processing model**.

Downstream deterministic deduplication handles possible duplicated events.

---

## ⚡ PySpark processing

The PySpark pipeline implements:

- explicit schemas;
- validation rules;
- Data Quality classification;
- valid/rejected data separation;
- deterministic deduplication;
- window functions;
- reference-data enrichment;
- broadcast joins;
- Parquet output;
- quality metrics;
- Quality Gates;
- structured operational logging.

Example validation rules include:

```text
missing aircraft identifier
missing timestamp
invalid latitude
invalid longitude
missing altitude
negative altitude
duplicate aircraft position
```

Rejected records are isolated rather than silently discarded.

---

## 🛡️ Data Quality

Data Quality is treated as part of the pipeline architecture.

The processing job calculates metrics including:

```text
raw_count
valid_count
rejected_count
rejection_rate
duplicate_rate
volume_variation_rate
freshness_minutes
global_status
```

Quality statuses are:

```text
OK
WARNING
FAILED
```

A critical Data Quality failure triggers the **Quality Gate** and stops the processing task.

This prevents invalid datasets from silently propagating downstream.

---

## 🔎 Observability

Each pipeline execution is correlated through an Airflow:

```text
run_id
```

The identifier is propagated to PySpark and persisted with monitoring artifacts.

```text
Airflow DagRun
      ↓
Task logs
      ↓
PySpark logs
      ↓
quality_metrics.json
      ↓
alert.json
```

This makes it possible to trace a specific logical execution across orchestration, processing, metrics and alerts.

Airflow's `try_number` distinguishes retries of the same logical execution.

---

## 🚨 Monitoring and alerting

Quality metrics are historised by processing date and `run_id`.

Example structure:

```text
data/metrics/aircraft_positions/
└── year=YYYY/
    └── month=MM/
        └── day=DD/
            └── run_id=<RUN_ID>/
                └── quality_metrics.json
```

Critical Data Quality failures also generate an alert artifact:

```text
data/alerts/aircraft_positions/
└── processing_date=YYYY-MM-DD/
    └── run_id=<RUN_ID>/
        └── alert.json
```

The current implementation uses local JSON artifacts to keep the monitoring layer generic and testable.

A production implementation could route these alerts to Cloud Monitoring, Slack, Microsoft Teams or an incident-management platform.

---

## 🔄 Retry, idempotence and recovery

The project explicitly distinguishes:

```text
Retry
= execute an operation again

Idempotence
= executing the same operation several times
  results in the same final state
```

Recovery is designed around the last successfully persisted stage.

Example:

```text
RAW        SUCCESS
PYSPARK    SUCCESS
BIGQUERY   FAILED
DBT        NOT_STARTED
```

The expected recovery strategy is:

```text
SKIP RAW
SKIP PYSPARK
RUN BIGQUERY
RUN DBT
```

A checkpoint prototype is available in:

```text
spark/jobs/pipeline_state.py
```

It demonstrates persisted pipeline state and recovery logic.

Airflow already maintains task state, so the prototype is intentionally not presented as a second production source of truth.

---

## 🧪 Automated tests

The PySpark processing layer is covered by automated tests.

Current test suite:

```text
14 tests
```

It covers:

- altitude validation;
- deterministic deduplication;
- reference-data enrichment;
- Quality Gate behavior;
- rejection-rate thresholds;
- duplicate-rate thresholds;
- volume controls;
- freshness controls;
- future timestamps;
- global quality-status priority.

Run the tests with:

```bash
/home/matde/.venvs/toulouse-aviation/bin/python \
  -m pytest -v spark/tests/
```

Current validated result:

```text
14 passed
```

---

## 🗄️ BigQuery

BigQuery provides the analytical warehouse layer.

The project includes:

- dedicated datasets;
- explicit schemas;
- date partitioning;
- clustering;
- Python loading;
- IAM-controlled service accounts.

The aircraft-position table is partitioned by timestamp and clustered by aircraft identifier to reduce unnecessary scans for common analytical access patterns.

---

## 🔧 dbt transformation layer

The dbt project is located in:

```text
aviation_dbt/
```

The transformation architecture follows:

```text
BigQuery RAW
     ↓
staging
     ↓
intermediate
     ↓
marts
```

The dbt implementation includes:

- sources;
- `source()`;
- `ref()`;
- staging models;
- intermediate transformations;
- marts;
- SQL tests;
- macros;
- documentation;
- lineage.

Environment-specific values such as GCP project and RAW dataset are externalized through environment variables.

---

## ⏱️ Airflow orchestration

The Airflow DAG is located at:

```text
airflow/dags/aviation_pipeline.py
```

Current local workflow:

```text
start
  ↓
get_processing_date
  ↓
check_raw_file
  ↓
run_pyspark
  ├──────────────→ quality_failure_diagnostic
  ↓
check_processed_data
  ↓
end
```

The DAG implements:

- parameterized processing dates;
- retries;
- retry delays;
- task dependencies;
- XCom usage;
- failure diagnostics;
- `run_id` propagation.

The current portfolio environment uses:

```text
schedule=None
```

for controlled manual demonstrations.

---

## ☁️ Infrastructure as Code

Terraform manages the GCP infrastructure.

Structure:

```text
terraform/
├── bootstrap/
│   └── state-bucket/
├── environments/
│   ├── dev/
│   └── prod/
└── modules/
    ├── bigquery/
    └── storage/
```

The implementation demonstrates:

- reusable Terraform modules;
- DEV/PROD separation;
- remote Terraform state;
- state locking considerations;
- GCS resources;
- BigQuery resources;
- service accounts;
- IAM;
- least-privilege principles.

Environment-specific state is isolated through separate backend prefixes.

---

## 🔐 Cloud authentication and security

The project avoids committing static cloud credentials.

Authentication and authorization use mechanisms including:

- Google Application Default Credentials;
- IAM;
- service accounts;
- Workload Identity Federation for CI/CD;
- least-privilege permissions.

Secrets and environment-specific configuration are kept outside source code.

The repository provides:

```text
.env.example
```

while real `.env` files and credential files are excluded from Git.

---

## 🐳 Docker

Docker is used for reproducible local execution.

The repository includes containers for:

- Kafka producer;
- Kafka consumer;
- Python demonstration services.

Kafka is orchestrated locally with Docker Compose.

The project covers:

- Docker images vs containers;
- Dockerfiles;
- build context;
- `.dockerignore`;
- environment variables;
- health checks;
- service dependencies;
- restart policies;
- Kafka listeners.

---

## 🔁 CI/CD

GitHub Actions provides automated validation.

Workflow:

```text
Pull Request / Push
        ↓
+----------------------+
| PySpark tests        |
| Terraform checks     |
| Docker checks        |
+----------------------+
        ↓
    Release Gate
```

The CI pipeline includes:

- Python/PySpark tests;
- Terraform formatting;
- Terraform initialization;
- Terraform validation;
- Terraform plan;
- Docker Compose validation;
- Docker image builds.

GCP authentication from GitHub Actions uses **Workload Identity Federation**, avoiding long-lived service-account keys.

The release gate demonstrates controlled delivery logic.

The repository does **not** claim fully automated production deployment.

---

## 🧱 Delta Lake / Lakehouse experiments

Delta Lake is explored locally as an alternative Lakehouse architecture.

Experiments include:

- Bronze tables;
- Silver transformations;
- Gold aggregations;
- ACID transactions;
- `_delta_log`;
- Time Travel;
- MERGE / UPSERT;
- schema enforcement;
- schema evolution.

Example scripts are available in:

```text
spark/jobs/delta_*.py
```

These are local Delta Lake experiments.

The project does **not** claim a deployed Databricks workspace.

---

## ❄️ Snowflake

Snowflake was studied as an alternative cloud Data Warehouse architecture, including:

- virtual warehouses;
- micro-partitions;
- pruning;
- stages;
- `COPY INTO`;
- Snowpipe;
- Streams;
- Tasks;
- Time Travel;
- zero-copy cloning;
- RBAC;
- cost management;
- Dynamic Tables;
- Secure Views;
- Data Sharing.

Snowflake is currently part of the architectural learning scope and is **not presented as a deployed component of this repository**.

---

## 📂 Repository structure

```text
toulouse-aviation-data-platform/
│
├── airflow/
│   └── dags/
│
├── aviation_dbt/
│
├── docker/
│   ├── kafka-consumer/
│   ├── kafka-producer/
│   └── python-demo/
│
├── docs/
│   ├── airflow/
│   ├── architecture/
│   ├── cicd/
│   ├── data-quality/
│   ├── databricks-delta/
│   ├── dbt/
│   ├── docker/
│   ├── gcp/
│   ├── kafka/
│   ├── operations/
│   ├── spark/
│   └── terraform/
│
├── ingestion/
│   ├── bigquery/
│   ├── gcs/
│   └── kafka/
│
├── spark/
│   ├── jobs/
│   └── tests/
│
├── terraform/
│   ├── bootstrap/
│   ├── environments/
│   └── modules/
│
├── .github/
│   └── workflows/
│
├── .env.example
├── docker-compose.yml
└── README.md
```

---

## 📚 Technical documentation

Detailed documentation is available under `docs/`.

Key documents include:

```text
docs/architecture/architecture.md
docs/gcp/gcp-fundamentals.md
docs/gcp/bigquery.md
docs/dbt/dbt-fundamentals.md
docs/kafka/kafka-fundamentals.md
docs/spark/pyspark-fundamentals.md
docs/airflow/airflow-orchestration.md
docs/terraform/terraform-iac.md
docs/docker/docker-containerization.md
docs/cicd/github-actions-cicd.md
docs/data-quality/data-quality-observability.md
docs/databricks-delta/databricks-delta-lake.md
docs/operations/production-runbook.md
```

The production runbook documents incident diagnosis, Quality Gate behavior, replay and recovery procedures.

---

## 🚑 Production runbook

Operational procedures are documented in:

```text
docs/operations/production-runbook.md
```

The runbook covers:

- technical vs Data Quality incidents;
- `run_id` correlation;
- monitoring;
- alerting;
- immutable RAW principles;
- retry vs idempotence;
- recovery;
- checkpoint strategy;
- replay procedures;
- configuration;
- IAM;
- CI/CD;
- known limitations.

---

## 🚧 Current scope and limitations

This repository is a **Data Engineering portfolio platform**, not a claim of a fully deployed enterprise production system.

### Implemented / tested

- Kafka producer and consumer;
- Kafka-to-GCS ingestion logic;
- GCS;
- BigQuery;
- dbt transformations;
- PySpark processing;
- automated Data Quality;
- pytest suite;
- Airflow DAG;
- Terraform infrastructure;
- Docker;
- GitHub Actions CI/CD;
- IAM / Workload Identity Federation;
- monitoring and alert artifacts;
- operational runbook.

### Local experiments

- Delta Lake;
- Bronze / Silver / Gold architecture;
- Time Travel;
- MERGE;
- schema evolution.

### Target / future evolution

- production Spark runtime;
- scheduled production Airflow deployment;
- centralized observability;
- external alert delivery;
- complete BigQuery/dbt downstream orchestration;
- end-to-end runtime integration testing;
- BI dashboards connected to the complete production flow.

This distinction is intentional so that the repository clearly separates **implemented engineering work from architectural targets**.

---

## 💡 Engineering concepts demonstrated

This project demonstrates practical understanding of:

```text
Batch vs Streaming
Kafka partitions and consumer groups
Offset management
At-least-once processing
Distributed Spark processing
Shuffle and partitioning
Broadcast joins
Window functions
Deterministic deduplication
Data Quality
Quality Gates
Observability
Idempotence
Retry strategies
Pipeline recovery
Data Warehousing
dbt lineage
Infrastructure as Code
Terraform state
IAM
Containerization
CI/CD
Cloud authentication
Lakehouse architecture
```

---

## 👤 Author

**Mathieu Delmas**

Data Engineer — Toulouse, France

Portfolio project focused on modern Data Engineering, cloud infrastructure and production-oriented data pipelines.