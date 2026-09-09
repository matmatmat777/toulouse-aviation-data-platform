# Module 10 — CI/CD avec GitHub Actions

## 1. Objectif

L'objectif de ce module est d'industrialiser les contrôles de la Toulouse Aviation Data Platform avec GitHub Actions.

Le pipeline doit automatiquement vérifier :

- les tests PySpark ;
- la configuration Terraform ;
- le plan Terraform sur l'infrastructure GCP réelle ;
- la configuration Docker Compose ;
- la construction des images Docker ;
- les conditions permettant d'autoriser une release.

Architecture générale :

```text
Feature branch
      │
      ▼
Pull Request
      │
      ▼
GitHub Actions CI
      │
      ├── PySpark tests
      ├── Terraform fmt / validate / plan
      └── Docker config / build
      │
      ▼
Quality Gates
      │
      ▼
Merge vers main
      │
      ▼
Nouvelle CI sur main
      │
      ▼
Release Gate
      │
      ▼
Validation humaine
      │
      ▼
Déploiement Production
```

---

# 2. CI et CD

## Continuous Integration — CI

La Continuous Integration consiste à intégrer régulièrement le code et à vérifier automatiquement sa qualité.

Dans ce projet, la CI contrôle notamment :

```text
Code
 ↓
Tests PySpark
 ↓
Terraform
 ↓
Docker
```

La CI détecte les erreurs.

Elle ne les corrige pas automatiquement.

Un contrôle qui échoue doit faire échouer le pipeline afin d'empêcher du code non valide de poursuivre le processus de livraison.

---

## Continuous Delivery — CD

Le projet utilise une approche **Continuous Delivery**.

Après validation de la CI, le système peut préparer une release, mais une validation humaine reste nécessaire avant une modification de l'infrastructure de production.

```text
CI
 ↓
Release candidate
 ↓
Validation humaine
 ↓
Production
```

À distinguer du **Continuous Deployment** :

```text
CI
 ↓
Déploiement automatique
 ↓
Production
```

Dans ce second modèle, aucune validation humaine n'est nécessaire si tous les quality gates sont validés.

Pour une infrastructure Terraform de production, le projet conserve volontairement un contrôle humain.

---

# 3. GitHub Actions

Les workflows GitHub Actions sont stockés dans :

```text
.github/workflows/
```

Workflow principal :

```text
.github/workflows/ci.yml
```

Un workflow est constitué de plusieurs jobs.

Exemple :

```text
Workflow
 │
 ├── Job tests-pyspark
 │     ├── checkout
 │     ├── Java
 │     ├── Python
 │     └── pytest
 │
 ├── Job terraform-check
 │     ├── checkout
 │     ├── authentication GCP
 │     ├── terraform fmt
 │     ├── terraform init
 │     ├── terraform validate
 │     └── terraform plan
 │
 ├── Job docker-check
 │     ├── checkout
 │     ├── compose config
 │     └── compose build
 │
 └── Job release-gate
```

---

# 4. Workflow, Job et Step

La hiérarchie GitHub Actions est :

```text
Workflow
   ↓
Jobs
   ↓
Steps
```

Exemple :

```yaml
jobs:
  tests-pyspark:
    runs-on: ubuntu-latest

    steps:
      - name: Checkout repository
        uses: actions/checkout@v4

      - name: Run tests
        run: python -m pytest
```

---

# 5. `uses` et `run`

## uses

`uses` permet d'utiliser une GitHub Action existante.

Exemple :

```yaml
uses: actions/checkout@v4
```

Cette action récupère le contenu du repository dans le runner.

Autre exemple :

```yaml
uses: actions/setup-python@v5
```

Elle prépare l'environnement Python.

---

## run

`run` exécute une commande shell directement sur le runner.

Exemple :

```yaml
run: terraform validate
```

ou :

```yaml
run: python -m pytest -v spark/tests/
```

Résumé :

```text
uses → exécute une Action GitHub réutilisable
run  → exécute une commande shell
```

---

# 6. GitHub Runner

Chaque job s'exécute dans son propre runner.

Dans notre workflow :

```text
tests-pyspark
      ↓
Runner Ubuntu A

terraform-check
      ↓
Runner Ubuntu B

docker-check
      ↓
Runner Ubuntu C
```

Les runners sont isolés.

Une dépendance installée dans un job n'est donc pas automatiquement disponible dans un autre job.

De même, les credentials obtenus dans un job ne sont pas automatiquement transmis aux autres jobs.

C'est pourquoi le job `terraform-check` doit effectuer lui-même son authentification GCP.

Cette isolation améliore également la reproductibilité et permet de détecter les dépendances implicites du type :

> "Ça fonctionne sur ma machine."

---

# 7. Quality Gates

Un quality gate est un contrôle obligatoire avant de poursuivre le pipeline.

Exemples dans le projet :

```text
PySpark tests      → Quality Gate
Terraform checks   → Quality Gate
Docker build       → Quality Gate
```

Si un contrôle obligatoire échoue :

```text
tests-pyspark     ✅
terraform-check  ❌
docker-check      ✅
```

la release ne doit pas être autorisée.

---

# 8. Dépendances entre jobs avec `needs`

GitHub Actions permet de définir des dépendances entre jobs avec :

```yaml
needs:
  - tests-pyspark
  - terraform-check
  - docker-check
```

Exemple :

```text
tests-pyspark ─────┐
                   │
terraform-check ───┼──► release-gate
                   │
docker-check ──────┘
```

Si un job obligatoire échoue, le job dépendant est ignoré.

Pendant les tests du projet, ce comportement a été observé directement :

```text
terraform-check  ❌
release-gate      SKIPPED
```

---

# 9. Tests PySpark dans la CI

Le job PySpark prépare Java et Python :

```yaml
- name: Set up Java
  uses: actions/setup-java@v5
  with:
    distribution: temurin
    java-version: "17"

- name: Set up Python
  uses: actions/setup-python@v5
  with:
    python-version: "3.12"
```

Puis installe les dépendances :

```bash
pip install pytest pyspark
```

et exécute :

```bash
python -m pytest -v spark/tests/
```

La CI a été testée volontairement avec un test incorrect.

Le résultat attendu a été observé :

```text
2 passed
1 failed
```

GitHub Actions a alors marqué le pipeline en échec.

Après correction du test :

```text
3 passed
```

le pipeline est redevenu vert.

Cela démontre le rôle de **quality gate** des tests automatisés.

---

# 10. Terraform dans la CI

Le job Terraform réalise plusieurs contrôles.

## Formatting

```bash
terraform fmt -check -recursive
```

Il vérifie que les fichiers Terraform respectent le format attendu.

Une erreur réelle a été détectée dans :

```text
terraform/modules/storage/variables.tf
```

Elle a été corrigée localement avec :

```bash
terraform fmt -recursive terraform
```

---

## terraform init

```bash
terraform init
```

Initialise notamment :

- les providers ;
- les modules ;
- le backend ;
- l'accès au remote state.

Dans le projet, le backend est stocké dans GCS.

---

## terraform validate

```bash
terraform validate
```

Vérifie la validité de la configuration Terraform.

Il ne détermine pas les modifications à effectuer sur l'infrastructure réelle.

---

## terraform plan

```bash
terraform plan -input=false
```

Le plan compare :

```text
Configuration Terraform souhaitée
              +
State Terraform
              +
Infrastructure GCP réelle
              ↓
        Terraform Plan
```

Il indique ensuite les changements nécessaires :

```text
+ create
~ update
- destroy
-/+ replace
```

Le `terraform plan` nécessite donc une authentification GCP.

---

# 11. Remote State Terraform

Le state DEV est stocké dans GCS :

```text
gs://toulouse-aviation-tfstate-408818015704
```

avec le préfixe :

```text
terraform/dev
```

Le state représente la connaissance que Terraform possède de l'infrastructure qu'il gère.

Le pipeline doit pouvoir consulter ce state afin de produire un plan cohérent.

---

# 12. State Lock

Terraform protège son state contre les opérations concurrentes.

Lors d'un `terraform plan`, Terraform a tenté de créer :

```text
terraform/dev/default.tflock
```

Ce fichier représente un verrou temporaire.

Exemple du problème évité :

```text
Utilisateur A
terraform apply
      │
      ├──────► même state
      │
Utilisateur B
terraform apply
```

Sans verrou, deux opérations concurrentes pourraient modifier simultanément l'infrastructure et rendre le state incohérent.

Avec le state lock :

```text
Utilisateur A
      ↓
LOCK
      ↓
Terraform operation
      ↓
UNLOCK
```

Les autres opérations doivent attendre.

Le compte CI doit donc disposer des permissions nécessaires pour gérer le verrou sur le bucket du state.

---

# 13. Authentification GitHub → GCP

Le projet n'utilise pas de clé JSON longue durée dans GitHub.

L'authentification utilise :

```text
GitHub Actions
      ↓
OIDC Token
      ↓
Workload Identity Federation
      ↓
Google Cloud
      ↓
Service Account
```

Service Account CI :

```text
github-cicd@toulouse-aviation-data.iam.gserviceaccount.com
```

Workload Identity Pool :

```text
github
```

Provider :

```text
github-provider
```

---

# 14. Pourquoi OIDC / Workload Identity Federation ?

Une solution traditionnelle serait de créer une clé JSON de Service Account et de la stocker dans GitHub Secrets.

Cela introduit un credential longue durée qu'il faut :

- stocker ;
- protéger ;
- faire tourner ;
- révoquer en cas de fuite.

Avec OIDC/WIF :

```text
GitHub workflow
      ↓
token OIDC temporaire
      ↓
GCP vérifie l'identité
      ↓
credentials temporaires
```

Aucune clé JSON GCP permanente n'est stockée dans le repository.

---

# 15. Permissions GitHub Actions

Le job Terraform utilise :

```yaml
permissions:
  contents: read
  id-token: write
```

## contents: read

Autorise le workflow à lire le contenu du repository.

Cette permission est notamment utilisée par :

```yaml
actions/checkout
```

## id-token: write

Autorise GitHub Actions à demander un token OIDC.

Ce token est ensuite utilisé pour l'authentification auprès de GCP via Workload Identity Federation.

Important :

```text
id-token: write
```

ne signifie pas :

```text
permission d'écrire dans GCP
```

Les permissions GCP sont gérées séparément par IAM.

---

# 16. IAM et Least Privilege

Le principe du moindre privilège consiste à ne donner à une identité que les permissions nécessaires à son travail.

Le compte CI n'a pas vocation à posséder arbitrairement des permissions administrateur.

Architecture :

```text
GitHub
  ↓
OIDC
  ↓
Service Account CI
  ↓
IAM
  ↓
Permissions nécessaires uniquement
```

Les permissions nécessaires au backend Terraform sont limitées au bucket de state concerné.

Le compte de CI et un futur compte de déploiement production peuvent également avoir des niveaux de permissions différents.

Un compte utilisé pour `terraform apply` représente un risque supérieur puisqu'il peut réellement :

```text
créer
modifier
supprimer
```

des ressources.

---

# 17. APIs GCP nécessaires

Pendant l'intégration CI, deux APIs distinctes ont dû être activées.

## IAM Service Account Credentials API

```text
iamcredentials.googleapis.com
```

Elle intervient notamment dans la chaîne permettant d'obtenir les credentials nécessaires à l'impersonation du Service Account avec notre configuration WIF.

## Identity and Access Management API

```text
iam.googleapis.com
```

Terraform en a eu besoin pour consulter les ressources IAM gérées dans l'infrastructure.

Ces erreurs ont permis de distinguer :

```text
Authentification
      ↓
Permissions IAM
      ↓
Disponibilité des APIs GCP
```

Une authentification correcte ne garantit donc pas qu'une API GCP nécessaire soit activée.

---

# 18. Docker dans la CI

Le job Docker réalise deux contrôles principaux.

Validation du fichier Compose :

```bash
docker compose config
```

Construction des images :

```bash
docker compose build
```

Architecture :

```text
docker-check
    │
    ├── docker compose config
    │
    └── docker compose build
```

Un build Docker réussi dans un runner propre améliore la confiance dans la reproductibilité des images.

---

# 19. Branches Git

Les développements ne doivent idéalement pas être réalisés directement sur `main`.

Exemple :

```bash
git checkout -b feature/ci-improvements
```

Architecture :

```text
main
 │
 └── feature/ci-improvements
           │
           ├── modifications
           ├── commits
           └── push
```

Le développeur travaille ainsi de manière isolée sans modifier directement la branche stable.

---

# 20. Pull Request

Après le développement :

```text
feature/ci-improvements
          ↓
     Pull Request
          ↓
        main
```

La Pull Request permet :

- l'exécution de la CI ;
- la revue de code ;
- la comparaison des changements ;
- la discussion ;
- la validation avant merge.

Une première Pull Request réelle a été créée dans ce projet pour modifier le pipeline CI/CD.

---

# 21. Cycle Git complet

Le workflow utilisé est :

```text
git checkout -b feature/xxx
        ↓
modifications
        ↓
git add
        ↓
git commit
        ↓
git push
        ↓
Pull Request
        ↓
CI
        ↓
Review
        ↓
Merge
        ↓
main
```

`main` représente ainsi le code intégré et validé.

---

# 22. Déclenchement du workflow

Configuration optimisée :

```yaml
on:
  push:
    branches:
      - main

  pull_request:
    branches:
      - main
```

Cela évite d'exécuter inutilement deux workflows identiques lors d'un push sur une branche ayant déjà une Pull Request.

Comportement :

```text
push feature               → pas de CI push
Pull Request vers main     → CI
mise à jour de la PR       → CI
merge vers main            → CI
push direct main           → CI
```

---

# 23. Release Gate

Le pipeline contient un job :

```text
release-gate
```

Il dépend des contrôles précédents :

```yaml
needs:
  - tests-pyspark
  - terraform-check
  - docker-check
```

Il n'est autorisé que pour un push sur `main` :

```yaml
if: github.event_name == 'push' && github.ref == 'refs/heads/main'
```

et est associé à :

```yaml
environment: production
```

Architecture :

```text
PySpark ───────┐
               │
Terraform ─────┼────► release-gate
               │
Docker ────────┘
```

Le job représente la frontière entre la CI et une future phase de déploiement.

---

# 24. Pourquoi ne pas exécuter automatiquement `terraform apply` ?

Le pipeline exécute :

```bash
terraform plan
```

mais pas :

```bash
terraform apply -auto-approve
```

sur chaque push.

La raison est simple :

```text
PLAN
 ↓
observe et prépare les changements

APPLY
 ↓
modifie réellement l'infrastructure
```

Une erreur dans un `apply` peut :

- supprimer une ressource ;
- modifier une configuration ;
- provoquer une interruption ;
- détruire des données si les protections sont insuffisantes.

Le projet utilise donc une approche :

```text
Automatisation
      +
Quality Gates
      +
Validation humaine
```

avant toute future modification de production.

---

# 25. Exemple de pipeline final

```yaml
name: Toulouse Aviation CI

on:
  push:
    branches:
      - main

  pull_request:
    branches:
      - main

jobs:
  tests-pyspark:
    runs-on: ubuntu-latest

    steps:
      - name: Checkout repository
        uses: actions/checkout@v4

      - name: Set up Java
        uses: actions/setup-java@v5
        with:
          distribution: temurin
          java-version: "17"

      - name: Set up Python
        uses: actions/setup-python@v5
        with:
          python-version: "3.12"

      - name: Install test dependencies
        run: |
          python -m pip install --upgrade pip
          pip install pytest pyspark

      - name: Run PySpark tests
        run: python -m pytest -v spark/tests/

  terraform-check:
    runs-on: ubuntu-latest

    permissions:
      contents: read
      id-token: write

    steps:
      - name: Checkout repository
        uses: actions/checkout@v4

      - name: Authenticate to Google Cloud
        uses: google-github-actions/auth@v3
        with:
          workload_identity_provider: projects/408818015704/locations/global/workloadIdentityPools/github/providers/github-provider
          service_account: github-cicd@toulouse-aviation-data.iam.gserviceaccount.com

      - name: Setup Terraform
        uses: hashicorp/setup-terraform@v3
        with:
          terraform_version: "1.16.1"

      - name: Check Terraform formatting
        run: terraform fmt -check -recursive
        working-directory: terraform

      - name: Initialize Terraform
        run: terraform init
        working-directory: terraform/environments/dev

      - name: Validate Terraform
        run: terraform validate
        working-directory: terraform/environments/dev

      - name: Plan Terraform
        run: terraform plan -input=false
        working-directory: terraform/environments/dev

  docker-check:
    runs-on: ubuntu-latest

    steps:
      - name: Checkout repository
        uses: actions/checkout@v4

      - name: Validate Docker Compose
        run: docker compose config

      - name: Build Docker images
        run: docker compose build

  release-gate:
    runs-on: ubuntu-latest

    needs:
      - tests-pyspark
      - terraform-check
      - docker-check

    if: github.event_name == 'push' && github.ref == 'refs/heads/main'

    environment: production

    steps:
      - name: Production release gate
        run: echo "CI passed - production deployment can be approved"
```

---

# 26. Incidents rencontrés pendant le module

Plusieurs erreurs réelles ont permis de comprendre le fonctionnement du pipeline.

## Terraform formatting

```text
terraform fmt -check -recursive
FAILED
```

Cause :

```text
terraform/modules/storage/variables.tf
```

Correction :

```bash
terraform fmt -recursive terraform
```

---

## Test PySpark volontairement cassé

Résultat :

```text
2 passed
1 failed
```

La CI est devenue rouge.

Après restauration du test :

```text
3 passed
```

la CI est redevenue verte.

---

## YAML incorrect

Une mauvaise indentation avait placé un job au mauvais niveau.

Résultat :

```text
workflow failure
0s
```

Cela démontre l'importance de la structure YAML.

---

## IAM Service Account Credentials API désactivée

Erreur :

```text
SERVICE_DISABLED
iamcredentials.googleapis.com
```

Correction :

```bash
gcloud services enable iamcredentials.googleapis.com \
  --project=toulouse-aviation-data
```

---

## State lock GCS

Erreur :

```text
Error acquiring the state lock
storage.objects.create denied
default.tflock
```

Cause :

le compte CI pouvait lire le state mais pas créer le verrou Terraform.

Les permissions du bucket de state ont été adaptées.

---

## IAM API désactivée

Erreur :

```text
SERVICE_DISABLED
iam.googleapis.com
```

Correction :

```bash
gcloud services enable iam.googleapis.com \
  --project=toulouse-aviation-data
```

Après ces corrections :

```text
Terraform init      PASS
Terraform validate  PASS
Terraform plan      PASS
```

---

# 27. Commandes GitHub CLI utiles

Lister les runs :

```bash
gh run list
```

Voir un run :

```bash
gh run view <RUN_ID>
```

Voir uniquement les logs en erreur :

```bash
gh run view <RUN_ID> --log-failed
```

Surveiller un run :

```bash
gh run watch <RUN_ID>
```

Relancer un run :

```bash
gh run rerun <RUN_ID>
```

Voir les contrôles d'une PR :

```bash
gh pr checks <PR_NUMBER>
```

Créer une PR :

```bash
gh pr create
```

Merger une PR :

```bash
gh pr merge <PR_NUMBER>
```

---

# 28. Compétences acquises

À l'issue de ce module :

- création d'un workflow GitHub Actions ;
- compréhension Workflow / Job / Step ;
- utilisation de `uses` et `run` ;
- runners GitHub isolés ;
- tests PySpark automatisés ;
- contrôle Docker automatisé ;
- Terraform fmt / init / validate / plan en CI ;
- remote state GCS ;
- state locking ;
- OIDC ;
- Workload Identity Federation ;
- authentification GitHub → GCP sans clé JSON permanente ;
- IAM et least privilege ;
- quality gates ;
- dépendances avec `needs` ;
- branches feature ;
- Pull Requests ;
- release gate ;
- distinction CI / Continuous Delivery / Continuous Deployment.

---

# 29. Points à réviser

## Permissions GitHub OIDC

```text
contents: read
→ lecture du repository

id-token: write
→ obtention d'un token OIDC
```

## Terraform State Lock

```text
State lock
→ empêche plusieurs opérations Terraform concurrentes
  sur le même state
```

## CI vs CD

```text
CI
→ intégrer et vérifier automatiquement le code

Continuous Delivery
→ préparer automatiquement une release
→ validation humaine avant production

Continuous Deployment
→ déployer automatiquement après validation de la CI
```

---

# 30. Réponse type entretien

> Sur la Toulouse Aviation Data Platform, j'ai mis en place une CI avec GitHub Actions. Les Pull Requests déclenchent des tests PySpark, des contrôles et un plan Terraform ainsi qu'une validation et un build Docker. Terraform utilise un remote state GCS et GitHub s'authentifie auprès de GCP via OIDC et Workload Identity Federation, sans clé de Service Account permanente dans GitHub. Les différents contrôles servent de quality gates. Après merge sur main, la CI est rejouée et un release gate associé à l'environnement de production prépare la phase de Continuous Delivery. La modification effective de l'infrastructure de production reste volontairement soumise à une validation humaine.

---

# 31. Résumé

```text
Developer
   ↓
Feature Branch
   ↓
Pull Request
   ↓
GitHub Actions
   │
   ├── PySpark Tests
   ├── Terraform Plan
   └── Docker Build
   ↓
Quality Gates
   ↓
Merge main
   ↓
Release Gate
   ↓
Human Approval
   ↓
Production
```

Le pipeline fournit une base CI/CD sécurisée, reproductible et adaptée à l'industrialisation progressive de la Toulouse Aviation Data Platform.