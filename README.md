# ETL Python Sales Project

ETL using Python to copy transactions data to a PostgreSQL DB.

## Estructura
- `generate_mock_data.py` -mock file to simulate data
- `etl.py` -main process
- `config.yaml` - parametrices info
- `db_schema.sql` - Schema and DB arquitecture
- `requirements.txt` - dependencies
- `Dockerfile`, `.dockerignore`

## Quickstart
1. Instala dependencias:
2. Genera mock data (opcional):
3. Ejecuta ETL:

## Git / GitHub
- Branch strategy: `main`, `feature/*`, `hotfix/*`
- Pull request template en `.github/PULL_REQUEST_TEMPLATE.md`

The main function is to load sales transactions froma a .csv fileto a SQL DB
    Include incremental control
    Data Quality
    Field Ecrypt
    Logging
    Error managing
    YAML file

Main features:
    Simulated sales data generation
    Change management without data lost
    YAML configuration
    Docker bucket
    Prepared for version control in GitHub

# Repo inicialitation
git init
git add .
git commit -m "Initial ETL project: mock data generator, ETL script, config, schema"

# create remote (ex: GitHub)
# manual repo creation --> `gh` CLI:
# gh repo create <user>/etl-python-sales-project --public --source=. --remote=origin

git branch -M main
git push -u origin main

# Branches Workflow
git checkout -b feature/incremental-load
# Changes
git add .
git commit -m "Improve incremental loading and parallel inserts"
git push origin feature/incremental-load
# PR creation in GitHub