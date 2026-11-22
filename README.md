# Job Intelligent

**Job Intelligent** is a data engineering and recommendation platform designed to collect, process, structure and analyze job offers from multiple sources in order to recommend relevant opportunities to users.

The project was developed as part of an academic Data Ingestion project in Digital Transformation and Artificial Intelligence.

---

## Project Overview

The goal of this project is to build a complete data pipeline for job-offer ingestion and recommendation.

The platform integrates job offers from multiple sources, cleans and standardizes the collected data, stores it in a structured architecture, and prepares it for analysis and recommendation.

Main objectives:

- Collect job offers from multiple sources
- Clean and standardize heterogeneous data
- Build a structured Data Lake
- Create a PostgreSQL Data Warehouse
- Orchestrate data workflows
- Prepare data for recommendation
- Visualize and analyze job-market information

---

## Data Sources

The project integrates job offers from several sources:

- Kaggle
- France Travail
- Rekrute
- Adzuna

Each source has its own format and structure, which requires normalization before integration.

---

## Data Pipeline

The pipeline follows several main steps:

### 1. Data Collection

Job offers are collected from multiple sources using:

- API integration
- Scraping scripts
- External datasets

---

### 2. Data Cleaning and Standardization

The collected data is cleaned and transformed to ensure consistency.

Main operations include:

- Missing value handling
- Duplicate removal
- Text normalization
- Job-title standardization
- Location standardization
- Source harmonization
- Data type conversion

---

### 3. Data Lake Architecture

The project uses a multi-zone Data Lake architecture.

```text
Raw Data
   ↓
Bronze
   ↓
Silver
   ↓
Gold


## Web dashboard (deployment)

The Flask interface (`interface/`) is a job-market analytics dashboard plus CV ↔ job matching.
`app.py` at the repository root is the entry point used by Vercel.

| Variable | Where | Purpose |
|---|---|---|
| `DATABASE_URL` | Vercel project env (server-side) | Postgres (Neon) holding table `jobs_clean` |
| `JOBS_CSV_PATH` | local only | Alternative: read a CSV instead of Postgres |

Load the processed offers into Postgres once (or from a scheduled job):

```bash
DATABASE_URL=postgresql://... python scripts/load_jobs_to_postgres.py data/processed/all_jobs_clean.csv
```

Run locally: `pip install -r requirements.txt && python app.py` → http://127.0.0.1:5000
Tests: `python -m pytest tests`.

Secrets live only in `.env` (git-ignored, see `.env.example`) or in the hosting provider's
environment settings. Never commit API keys.
