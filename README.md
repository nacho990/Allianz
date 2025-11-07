# ETL Python Sales Project

An ETL (Extract, Transform, Load) pipeline built in **Python** to process sales transaction data and load it into a **PostgreSQL** database.

---

## Project Structure

| File | Description |
|------|--------------|
| `generate_mock_data.py` | Mock data generator to simulate transactional sales data |
| `etl.py` | Main ETL process (extract, transform, load) |
| `config.yaml` | Configuration file for parameters and database connection |
| `db_schema.sql` | Database schema definition (tables, metadata, etc.) |
| `requirements.txt` | Python dependencies |
| `Dockerfile`, `.dockerignore` | Containerization setup for deployment |

---

## Quickstart

### Install Dependencies
Create a virtual environment and install required packages:
```bash
python -m venv venv
venv\Scripts\activate  # On Windows
source venv/bin/activate  # On Mac/Linux
pip install -r requirements.txt


Generate Mock Data
python generate_mock_data.py
python etl.py --config config.yaml

### Features

Incremental Load – Only loads new transactions since the last ETL run
Data Quality Validation – Detects missing values, duplicates, and outliers
Field Encryption – Optionally encrypts sensitive fields (e.g., customer ID)
Schema Evolution Handling – Stores unexpected columns as JSON in raw_payload
Error Handling & Logging – Detailed logs saved to /logs/etl.log
Configurable via YAML – All ETL parameters are externalized
Parallel Inserts – Supports concurrent data loading for scalability
Docker-Ready – Fully containerized setup for reproducible environments


### Version Control and Git Workflow

git init
git add .
git commit -m "Initial ETL project: mock data generator, ETL script, config, schema"

git remote add origin https://github.com/<your-username>/etl-python-sales-project.git
git branch -M main
git push -u origin main
git checkout -b feature/incremental-load
# Make changes
git add .
git commit -m "Improve incremental loading and parallel inserts"
git push origin feature/incremental-load