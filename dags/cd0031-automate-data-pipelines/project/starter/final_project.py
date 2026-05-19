"""
Sparkify ETL DAG
================
Automated data pipeline that extracts JSON logs and song metadata from S3,
stages them in Amazon Redshift, transforms them into a star schema
(fact + dimension tables), and runs data quality validation.

Architecture:
    S3 (JSON) → Staging Tables → Star Schema → Quality Checks

Schedule: Hourly
Author: Youssef El-Labed
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.empty import EmptyOperator

from final_project_operators.stage_redshift import StageToRedshiftOperator
from final_project_operators.load_fact import LoadFactOperator
from final_project_operators.load_dimension import LoadDimensionOperator
from final_project_operators.data_quality import DataQualityOperator

from udacity.common.final_project_sql_statements import SqlQueries


# ── Configuration ─────────────────────────────────────────────
S3_BUCKET = "sparkify-airflow-youssef"
AWS_REGION = "us-east-1"
REDSHIFT_CONN = "redshift"
AWS_CONN = "aws_credentials"

default_args = {
    "owner": "youssef",
    "depends_on_past": False,
    "start_date": datetime(2019, 1, 1),
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "email_on_retry": False,
    "email_on_failure": False,
}

with DAG(
    "sparkify_etl_pipeline",
    default_args=default_args,
    description="Load and transform data in Redshift with Airflow",
    schedule_interval="@hourly",
    catchup=False,
    max_active_runs=1,
    tags=["sparkify", "etl", "redshift", "s3"],
) as dag:

    # ── Pipeline bookends ─────────────────────────────────────
    start_execution = EmptyOperator(task_id="begin_execution")
    end_execution = EmptyOperator(task_id="end_execution")

    # ── STAGE: S3 → Redshift ──────────────────────────────────
    stage_events = StageToRedshiftOperator(
        task_id="stage_events_to_redshift",
        table="staging_events",
        s3_bucket=S3_BUCKET,
        s3_key="log-data",
        json_path=f"s3://{S3_BUCKET}/log_json_path.json",
        redshift_conn_id=REDSHIFT_CONN,
        aws_credentials_id=AWS_CONN,
        region=AWS_REGION,
    )

    stage_songs = StageToRedshiftOperator(
        task_id="stage_songs_to_redshift",
        table="staging_songs",
        s3_bucket=S3_BUCKET,
        s3_key="song-data",
        json_path="auto",
        redshift_conn_id=REDSHIFT_CONN,
        aws_credentials_id=AWS_CONN,
        region=AWS_REGION,
    )

    # ── TRANSFORM: Fact table ─────────────────────────────────
    load_songplays_fact = LoadFactOperator(
        task_id="load_songplays_fact_table",
        redshift_conn_id=REDSHIFT_CONN,
        table="songplays",
        sql=SqlQueries.songplay_table_insert,
    )

    # ── TRANSFORM: Dimension tables ──────────────────────────
    load_user_dim = LoadDimensionOperator(
        task_id="load_user_dim_table",
        redshift_conn_id=REDSHIFT_CONN,
        table="users",
        sql=SqlQueries.user_table_insert,
        truncate_insert=True,
    )

    load_song_dim = LoadDimensionOperator(
        task_id="load_song_dim_table",
        redshift_conn_id=REDSHIFT_CONN,
        table="songs",
        sql=SqlQueries.song_table_insert,
        truncate_insert=True,
    )

    load_artist_dim = LoadDimensionOperator(
        task_id="load_artist_dim_table",
        redshift_conn_id=REDSHIFT_CONN,
        table="artists",
        sql=SqlQueries.artist_table_insert,
        truncate_insert=True,
    )

    load_time_dim = LoadDimensionOperator(
        task_id="load_time_dim_table",
        redshift_conn_id=REDSHIFT_CONN,
        table="time",
        sql=SqlQueries.time_table_insert,
        truncate_insert=True,
    )

    # ── VALIDATE: Data quality checks ─────────────────────────
    dq_checks = [
        # Ensure no NULL primary keys across all tables
        {"sql": "SELECT COUNT(*) FROM songplays WHERE songplay_id IS NULL", "expected_result": 0},
        {"sql": "SELECT COUNT(*) FROM users WHERE userid IS NULL", "expected_result": 0},
        {"sql": "SELECT COUNT(*) FROM songs WHERE song_id IS NULL", "expected_result": 0},
        {"sql": "SELECT COUNT(*) FROM artists WHERE artist_id IS NULL", "expected_result": 0},
        {"sql": "SELECT COUNT(*) FROM time WHERE start_time IS NULL", "expected_result": 0},
    ]

    run_quality_checks = DataQualityOperator(
        task_id="run_data_quality_checks",
        redshift_conn_id=REDSHIFT_CONN,
        dq_checks=dq_checks,
    )

    # ── DAG Dependencies ──────────────────────────────────────
    start_execution >> [stage_events, stage_songs]
    [stage_events, stage_songs] >> load_songplays_fact
    load_songplays_fact >> [load_user_dim, load_song_dim, load_artist_dim, load_time_dim]
    [load_user_dim, load_song_dim, load_artist_dim, load_time_dim] >> run_quality_checks
    run_quality_checks >> end_execution