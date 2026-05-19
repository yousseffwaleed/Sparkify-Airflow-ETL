# Sparkify ETL Pipeline — Apache Airflow + Amazon Redshift

An automated data pipeline built with **Apache Airflow** that extracts JSON event logs and song metadata from **Amazon S3**, stages them in **Amazon Redshift**, transforms them into an analytics-ready **star schema**, and validates data quality.

![Python](https://img.shields.io/badge/Python-3.8+-blue)
![Airflow](https://img.shields.io/badge/Apache%20Airflow-2.x-green)
![AWS](https://img.shields.io/badge/AWS-Redshift%20%7C%20S3-orange)

---

## Architecture

```
                    ┌────────────────────────────────┐
                    │          Apache Airflow         │
                    │         (Orchestrator)          │
                    └──────────┬─────────────────────┘
                               │
          ┌────────────────────┼────────────────────┐
          ▼                    ▼                    ▼
   ┌─────────────┐    ┌──────────────┐    ┌──────────────┐
   │   Amazon S3  │    │   Redshift   │    │   Redshift   │
   │  (Raw JSON)  │───▶│  (Staging)   │───▶│ (Star Schema)│
   └─────────────┘    └──────────────┘    └──────────────┘
    log-data/           staging_events      songplays (fact)
    song-data/          staging_songs       users, songs,
                                            artists, time
```

## DAG Workflow

```
begin_execution
       │
       ├──▶ stage_events_to_redshift ──┐
       └──▶ stage_songs_to_redshift  ──┤
                                       ▼
                          load_songplays_fact_table
                                       │
                    ┌──────┬───────┬────┴────┐
                    ▼      ▼       ▼         ▼
                 users   songs  artists    time
                    │      │       │         │
                    └──────┴───────┴────┬────┘
                                       ▼
                          run_data_quality_checks
                                       │
                                       ▼
                               end_execution
```

## Star Schema

| Table | Type | Description |
|-------|------|-------------|
| **songplays** | Fact | Each row = one song play event (who, what, when, where) |
| **users** | Dimension | User profiles — name, gender, subscription level |
| **songs** | Dimension | Song metadata — title, artist, year, duration |
| **artists** | Dimension | Artist info — name, location, coordinates |
| **time** | Dimension | Timestamp breakdowns — hour, day, week, month, year |

## Project Structure

```
├── dags/
│   ├── cd0031-automate-data-pipelines/
│   │   ├── docker-compose.yaml          # Airflow local deployment
│   │   ├── requirements.txt             # Python dependencies
│   │   └── project/starter/
│   │       └── final_project.py         # DAG definition
│   └── udacity/common/
│       └── final_project_sql_statements.py  # SQL (CREATE + INSERT)
│
├── plugins/
│   └── final_project_operators/
│       ├── stage_redshift.py            # S3 → Redshift COPY operator
│       ├── load_fact.py                 # Fact table loader (append-only)
│       ├── load_dimension.py            # Dimension loader (truncate-insert)
│       └── data_quality.py             # Configurable quality checks
│
└── README.md
```

## Custom Operators

### `StageToRedshiftOperator`
Bulk-loads JSON files from S3 into Redshift staging tables using the `COPY` command. Clears the target table before each load for idempotent runs.

### `LoadFactOperator`
Populates the fact table with an `INSERT INTO ... SELECT` that joins staging tables. Append-only — no truncation, because fact tables grow over time.

### `LoadDimensionOperator`
Loads dimension tables with a configurable `truncate_insert` flag. When `True` (default), the table is wiped and fully refreshed each run.

### `DataQualityOperator`
Runs a configurable list of SQL-based checks (e.g., "no NULL primary keys"). Collects all failures and reports them together. Any failure triggers Airflow retries.

## Setup

### Prerequisites
- AWS account with an active **Redshift** cluster
- An **S3 bucket** with the Sparkify datasets (`log-data/`, `song-data/`)
- **Docker** installed (for local Airflow)

### 1. Configure Airflow Connections

In the Airflow UI (**Admin → Connections**), create:

| Connection ID | Type | Details |
|---------------|------|---------|
| `aws_credentials` | Amazon Web Services | IAM access key + secret key |
| `redshift` | Postgres | Host, port (5439), DB name, user, password |

### 2. Create Redshift Tables

Run the `CREATE TABLE` statements from `final_project_sql_statements.py` against your Redshift cluster to set up the schema.

### 3. Start Airflow

```bash
cd dags/cd0031-automate-data-pipelines
docker-compose up -d
```

### 4. Trigger the DAG

Open `http://localhost:8080`, find `sparkify_etl_pipeline`, and toggle it on.

## Technologies

- **Apache Airflow** — workflow orchestration, scheduling, retries
- **Amazon Redshift** — columnar data warehouse (OLAP)
- **Amazon S3** — raw data lake storage
- **Python** — operator logic, DAG definition
- **SQL** — data transformations (INSERT...SELECT, JOINs, COPY)

## Author

**Youssef ElShenawy**
