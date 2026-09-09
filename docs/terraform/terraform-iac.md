# Module 8 — Terraform & Infrastructure as Code

## Toulouse Aviation Data Platform

**Durée : ~12 h**  
**Statut : Terminé**

---

## 1. Objectif du module

L'objectif de ce module est de gérer l'infrastructure GCP de la Toulouse Aviation Data Platform avec Terraform.

L'infrastructure est décrite sous forme de code afin de la rendre :

- reproductible ;
- versionnable ;
- auditable ;
- réutilisable ;
- déployable sur plusieurs environnements.

Les ressources principales gérées sont :

- buckets Google Cloud Storage ;
- datasets BigQuery ;
- Service Accounts ;
- permissions IAM ;
- remote state Terraform ;
- environnements DEV et PROD.

---

# 2. Infrastructure as Code

Terraform permet de décrire l'infrastructure dans des fichiers `.tf` écrits en **HCL — HashiCorp Configuration Language**.

Exemple :

```hcl
resource "google_storage_bucket" "this" {
  name     = var.bucket_name
  location = var.location
}
```

L'infrastructure devient alors du code pouvant être stocké dans Git.

Le workflow principal est :

```text
Code Terraform
      ↓
terraform init
      ↓
terraform validate
      ↓
terraform plan
      ↓
Review
      ↓
terraform apply
      ↓
Infrastructure GCP
```

---

# 3. Provider

Un provider permet à Terraform de communiquer avec une plateforme externe.

Dans ce projet :

```hcl
terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 7.0"
    }
  }
}

provider "google" {
  project = var.project_id
  region  = var.region
}
```

Le provider Google permet à Terraform de gérer les ressources GCP.

---

# 4. Resources

Une `resource` représente un objet d'infrastructure géré par Terraform.

Exemple :

```hcl
resource "google_storage_bucket" "this" {
  name          = var.bucket_name
  location      = var.location
  storage_class = var.storage_class
}
```

Dans ce cas, Terraform gère réellement un bucket GCS.

Une ressource possède également une **adresse Terraform**.

Exemple :

```text
module.rejected_data.google_storage_bucket.this
```

Cette adresse permet notamment à Terraform de retrouver la ressource dans son state.

---

# 5. Variables

Les variables permettent de rendre la configuration paramétrable.

Exemple :

```hcl
variable "region" {
  description = "Default GCP region"
  type        = string
  default     = "europe-west9"
}
```

Elles peuvent être alimentées via un fichier :

```text
terraform.tfvars
```

Exemple :

```hcl
project_id = "toulouse-aviation-data"
region     = "europe-west9"
```

---

# 6. Locals

Les `locals` permettent de centraliser des valeurs internes à une configuration Terraform.

Exemple :

```hcl
locals {
  raw_bucket_name = "toulouse-aviation-data-raw"
}
```

Puis :

```hcl
bucket_name = local.raw_bucket_name
```

---

# 7. Outputs

Les outputs exposent des informations utiles après le déploiement.

Exemple :

```hcl
output "rejected_data_url" {
  description = "GCS URL of the rejected data"
  value       = module.rejected_data.bucket_url
}
```

Résultat :

```text
rejected_data_url = "gs://toulouse-aviation-data-rejected"
```

Un ajout d'output peut apparaître avec `+` dans :

```text
Changes to Outputs
```

Cela ne signifie pas nécessairement qu'une nouvelle ressource cloud est créée.

---

# 8. Modules Terraform

Un module permet d'encapsuler et de réutiliser une configuration Terraform.

Le projet contient notamment :

```text
terraform/
├── bootstrap/
│   └── state-bucket/
├── environments/
│   ├── dev/
│   └── prod/
└── modules/
    ├── storage/
    └── bigquery/
```

Le module `storage` contient la définition générique d'un bucket GCS.

Il peut ensuite être utilisé plusieurs fois.

### RAW

```hcl
module "storage" {
  source = "../../modules/storage"

  bucket_name = local.raw_bucket_name
  location    = var.region
}
```

### PROCESSED

```hcl
module "processed_storage" {
  source = "../../modules/storage"

  bucket_name = "toulouse-aviation-data-processed"
  location    = var.region
}
```

### REJECTED

```hcl
module "rejected_data" {
  source = "../../modules/storage"

  bucket_name = "toulouse-aviation-data-rejected"
  location    = var.region
}
```

Le même module générique est donc utilisé pour plusieurs instances.

Conceptuellement :

```text
             storage module
                   │
        ┌──────────┼──────────┐
        ↓          ↓          ↓
       RAW     PROCESSED   REJECTED
```

---

# 9. Paramétrage du module Storage

Le module expose notamment :

```hcl
variable "bucket_name" {
  description = "Name of the GCS bucket"
  type        = string
}

variable "location" {
  description = "GCS bucket location"
  type        = string
}

variable "storage_class" {
  description = "Storage class name"
  type        = string
  default     = "STANDARD"
}
```

La ressource utilise ensuite :

```hcl
storage_class = var.storage_class
```

Cela permet de modifier le comportement du module sans dupliquer sa définition.

---

# 10. Terraform State

Le **state est la mémoire de Terraform**.

Il permet de faire le lien entre :

```text
Configuration Terraform
          ↕
Terraform State
          ↕
Infrastructure réelle
```

Terraform utilise notamment le state pour déterminer quelles ressources il gère.

Commandes utiles :

```bash
terraform state list
terraform state show <adresse>
```

---

# 11. Remote State

Un state local dépend du poste sur lequel Terraform est exécuté.

Le projet utilise donc un backend GCS dédié :

```text
toulouse-aviation-tfstate-408818015704
```

Le bucket est :

- privé ;
- versionné ;
- protégé contre la destruction Terraform ;
- utilisé comme backend centralisé.

---

# 12. Bootstrap du Remote State

Le bucket contenant le state doit lui-même être créé avant que Terraform puisse l'utiliser comme backend.

Un root module séparé a donc été créé :

```text
terraform/bootstrap/state-bucket/
```

Exemple :

```hcl
resource "google_storage_bucket" "terraform_state" {
  name          = "toulouse-aviation-tfstate-408818015704"
  location      = "EUROPE-WEST9"
  storage_class = "STANDARD"

  uniform_bucket_level_access = true
  public_access_prevention    = "enforced"

  versioning {
    enabled = true
  }

  lifecycle {
    prevent_destroy = true
  }
}
```

Le state de ce bootstrap reste distinct du backend qu'il crée.

---

# 13. Backend GCS

DEV utilise :

```hcl
backend "gcs" {
  bucket = "toulouse-aviation-tfstate-408818015704"
  prefix = "terraform/dev"
}
```

PROD utilise :

```hcl
backend "gcs" {
  bucket = "toulouse-aviation-tfstate-408818015704"
  prefix = "terraform/prod"
}
```

Les deux environnements utilisent donc le même bucket physique mais des **states indépendants**.

```text
Terraform state bucket
│
├── terraform/dev
│
└── terraform/prod
```

Cela évite qu'une modification de DEV affecte le state de PROD.

---

# 14. Migration vers le Remote State

La migration du state DEV a été réalisée avec :

```bash
terraform init -migrate-state
```

Terraform transfère alors le state vers le backend configuré.

---

# 15. Import de ressources existantes

Une partie de l'infrastructure existait avant Terraform.

Il fallait donc l'intégrer au state plutôt que tenter de la recréer.

Workflow :

```text
Déclarer la resource
        ↓
terraform validate
        ↓
terraform import
        ↓
terraform plan
        ↓
Aligner la configuration
        ↓
No changes
```

Des ressources existantes ont ainsi été importées, notamment :

- bucket RAW ;
- dataset BigQuery RAW ;
- Service Account ;
- permission IAM.

`terraform import` associe une ressource cloud existante à une adresse Terraform.

---

# 16. Refactoring avec state mv

Le bucket RAW était initialement géré directement :

```text
google_storage_bucket.raw
```

Il a ensuite été déplacé dans le module Storage :

```text
module.storage.google_storage_bucket.this
```

La commande :

```bash
terraform state mv \
  google_storage_bucket.raw \
  module.storage.google_storage_bucket.this
```

permet de modifier son adresse dans le state.

Cela évite que Terraform interprète le refactoring comme :

```text
ancienne ressource supprimée
+
nouvelle ressource créée
```

Le bucket réel n'est pas recréé.

---

# 17. state rm

La commande :

```bash
terraform state rm <adresse>
```

demande à Terraform d'arrêter de suivre une ressource dans le state.

Elle **ne détruit pas la ressource cloud**.

Attention : si la ressource reste déclarée dans le HCL, un prochain `terraform plan` pourra proposer de la gérer/créer à nouveau.

---

# 18. Drift

Le drift correspond à une différence entre l'état désiré et l'infrastructure réelle.

Exemple :

```text
Terraform code : bucket doit exister
State          : bucket géré
GCP            : bucket supprimé manuellement
```

Au prochain :

```bash
terraform plan
```

Terraform détectera la différence et pourra proposer de recréer le bucket.

Cela montre pourquoi les modifications manuelles de l'infrastructure doivent être limitées.

---

# 19. Lecture d'un terraform plan

Symboles importants :

```text
+    création
~    modification sur place
-    destruction
-/+  destruction puis recréation
```

Exemple :

```text
Plan: 1 to add, 0 to change, 0 to destroy.
```

signifie qu'une seule nouvelle ressource sera créée.

Une opération `-/+` doit être examinée avec une attention particulière, surtout en production.

---

# 20. Exemple de modification in-place

Pendant le module, le bucket PROCESSED a temporairement été configuré avec :

```hcl
storage_class = "NEARLINE"
```

Terraform a proposé :

```text
~
```

La ressource pouvait être modifiée sans être détruite.

La modification n'a pas été appliquée et la configuration a été remise à `STANDARD`.

---

# 21. IAM et Service Accounts

Terraform gère également les identités et permissions.

Exemple :

```hcl
resource "google_service_account" "bigquery_loader" {
  account_id   = "aviation-bigquery-loader"
  display_name = "aviation-bigquery-loader"
  description  = "Loads raw aircraft position data from GCS into BigQuery"
}
```

Permission :

```hcl
resource "google_bigquery_dataset_iam_member" "raw_loader_writer" {
  project    = var.project_id
  dataset_id = module.bigquery_raw.dataset_id

  role   = "roles/bigquery.dataEditor"
  member = "serviceAccount:${google_service_account.bigquery_loader.email}"
}
```

---

# 22. Least Privilege

Le projet applique le principe du **moindre privilège**.

Une identité reçoit :

- uniquement les permissions nécessaires ;
- au périmètre le plus restreint possible.

Par exemple :

```text
BigQuery Loader
      ↓
roles/bigquery.dataEditor
      ↓
aviation_raw
```

plutôt qu'un rôle administrateur sur tout le projet GCP.

---

# 23. DEV et PROD

Deux environnements Terraform ont été créés :

```text
terraform/environments/dev/
terraform/environments/prod/
```

DEV utilise notamment :

```text
aviation_raw
toulouse-aviation-data-raw
toulouse-aviation-data-processed
toulouse-aviation-data-rejected
```

PROD possède ses propres ressources, notamment :

```text
aviation_prod_raw
toulouse-aviation-data-prod-raw
aviation-prod-bigquery-loader
```

Le state est également séparé entre les deux environnements.

---

# 24. Infrastructure Data DEV

L'infrastructure cible côté stockage est maintenant :

```text
Kafka / ingestion
       │
       ▼
┌───────────────────────┐
│       RAW GCS         │
└──────────┬────────────┘
           │
         PySpark
           │
      ┌────┴─────┐
      ▼          ▼
 PROCESSED     REJECTED
    GCS           GCS
```

Buckets :

```text
RAW
gs://toulouse-aviation-data-raw

PROCESSED
gs://toulouse-aviation-data-processed

REJECTED
gs://toulouse-aviation-data-rejected
```

À ce stade, Terraform provisionne ces buckets ; le job PySpark d'entraînement écrit encore sur le filesystem local et n'est pas encore connecté à ces destinations GCS.

---

# 25. Protection avec lifecycle

Terraform permet de protéger certaines ressources sensibles :

```hcl
lifecycle {
  prevent_destroy = true
}
```

Cette protection est utilisée pour le bucket contenant le remote state.

Elle permet de bloquer certaines opérations Terraform qui entraîneraient sa destruction tant que cette règle est présente dans la configuration.

Elle ne remplace pas :

- les permissions IAM ;
- les sauvegardes/versioning ;
- la revue du plan ;
- les protections organisationnelles.

---

# 26. terraform init

`terraform init` initialise un working directory Terraform.

Il est notamment nécessaire lors :

- de la première utilisation ;
- d'un changement de backend ;
- de l'ajout/changement de provider ;
- de l'ajout d'un module qui n'est pas encore installé/initialisé.

Exemple rencontré :

```text
Error: Module not installed
```

après l'ajout d'une nouvelle instance du module Storage.

Solution :

```bash
terraform init
```

---

# 27. terraform fmt

Commande :

```bash
terraform fmt -recursive
```

Elle formate automatiquement le HCL.

Pour simplement vérifier :

```bash
terraform fmt -check -recursive
```

Cette vérification pourra être intégrée au CI/CD.

---

# 28. terraform validate

Commande :

```bash
terraform validate
```

Elle vérifie que la configuration Terraform est syntaxiquement et structurellement valide.

Elle ne signifie pas que le déploiement sera sans impact.

---

# 29. terraform plan

Commande :

```bash
terraform plan
```

Elle calcule les changements nécessaires pour passer de l'état actuel à l'état désiré.

Le plan doit être lu avant chaque déploiement.

Exemple attendu :

```text
Plan: 1 to add, 0 to change, 0 to destroy.
```

---

# 30. terraform apply

Après validation du plan :

```bash
terraform apply
```

Terraform applique les modifications.

Exemple obtenu lors de la création du bucket REJECTED :

```text
Apply complete! Resources: 1 added, 0 changed, 0 destroyed.
```

---

# 31. Workflow retenu

Workflow quotidien :

```text
Modification HCL
      ↓
terraform fmt
      ↓
terraform validate
      ↓
terraform plan
      ↓
Analyse du plan
      ↓
terraform apply
      ↓
terraform plan
      ↓
No changes
```

En cas d'ajout/changement nécessitant une réinitialisation :

```text
Modification
    ↓
terraform init
    ↓
validate
    ↓
plan
    ↓
apply
```

---

# 32. Sécurité Git

Les fichiers de state ne doivent pas être versionnés.

`.gitignore` :

```gitignore
# Terraform
**/.terraform/*
*.tfstate
*.tfstate.*
.terraform.tfstate.lock.info
crash.log
crash.*.log
```

En revanche :

```text
.terraform.lock.hcl
```

doit être conservé dans Git afin de stabiliser les versions des providers utilisées.

Les fichiers contenant des secrets ou credentials ne doivent jamais être commités.

---

# 33. CI/CD — préparation

Une pipeline CI/CD Terraform pourra ensuite exécuter automatiquement :

```text
terraform fmt -check
        ↓
terraform validate
        ↓
terraform plan
```

pour DEV et PROD.

L'implémentation GitHub Actions sera réalisée dans le module CI/CD afin de conserver une séparation claire entre apprentissage Terraform et apprentissage CI/CD.

---

# 34. Points d'attention

Avant un `terraform apply`, vérifier systématiquement :

```text
+   Est-ce que je veux réellement créer cette ressource ?

~   Est-ce que cette modification in-place est attendue ?

-   Pourquoi Terraform veut-il détruire cette ressource ?

-/+ Pourquoi Terraform doit-il remplacer cette ressource ?
```

En production, une destruction ou un remplacement inattendu doit bloquer l'application jusqu'à compréhension de sa cause.

---

# 35. Compétences acquises

À l'issue du module :

- comprendre Infrastructure as Code ;
- écrire et modifier du HCL ;
- utiliser un provider ;
- créer des resources ;
- utiliser variables, locals et outputs ;
- construire et réutiliser des modules ;
- comprendre le Terraform state ;
- utiliser un remote state GCS ;
- séparer les states DEV et PROD ;
- importer une infrastructure existante ;
- utiliser `state mv` et `state rm` ;
- comprendre le drift ;
- gérer des Service Accounts et IAM ;
- appliquer le least privilege ;
- lire un `terraform plan` ;
- distinguer `+`, `~`, `-` et `-/+` ;
- utiliser `fmt`, `validate`, `plan` et `apply` ;
- protéger une ressource sensible avec `prevent_destroy`.

---

# 36. Réponse entretien

> J'ai repris une infrastructure GCP existante sous Terraform en important les ressources dans le state, puis j'ai refactoré les ressources GCS et BigQuery en modules réutilisables avec `terraform state mv`, sans recréation de l'infrastructure. J'ai centralisé le state dans un backend GCS versionné, séparé les states DEV et PROD par préfixe et géré les Service Accounts et IAM en appliquant le principe du moindre privilège. J'utilise systématiquement `fmt`, `validate` et surtout `plan` avant les déploiements afin de contrôler les changements d'infrastructure.

---

# 37. Points à réviser

Points à consolider :

### `~` vs `-/+`

```text
~    modification sur place
-/+  destruction puis recréation
```

### Import

```text
Ressource déjà existante
        ↓
déclaration HCL
        ↓
terraform import
        ↓
terraform plan
```

### State

```text
State = mémoire de Terraform
```

### Init

Penser à `terraform init` lorsqu'une modification concerne notamment les modules, providers ou backend.

---

# 38. Résultat du module

**Durée : ~12 heures**

Infrastructure Terraform opérationnelle avec :

- GCS RAW ;
- GCS PROCESSED ;
- GCS REJECTED ;
- BigQuery ;
- Service Accounts ;
- IAM ;
- modules réutilisables ;
- DEV ;
- PROD ;
- remote state GCS ;
- séparation des states ;
- protection du bucket de state.

**Module 8 — Terraform : TERMINÉ**

Prochaine étape :

**Module 9 — Docker**