"""
DAG Configuration Settings
Centralized configuration for the TikTok ETL pipeline.
"""

from datetime import timedelta

# ──────────────────────────────────────────────────────────────
# DAG SETTINGS
DAG_ID = "tiktok_etl_dag"
DAG_DESCRIPTION = "TikTok Hashtag Data Warehouse ETL Pipeline"
DAG_TAGS = ["tiktok", "etl", "social-media", "production"]
DAG_OWNER = "dat"
DAG_START_DATE = "2024-01-01"
DAG_SCHEDULE = "@daily"
DAG_CATCHUP = False
DAG_MAX_ACTIVE_RUNS = 1

# ──────────────────────────────────────────────────────────────
# TASK SETTINGS
# ──────────────────────────────────────────────────────────────
TASK_RETRIES = 2
TASK_RETRY_DELAY = timedelta(seconds=30)
TASK_EXECUTION_TIMEOUT = timedelta(minutes=10)

# ──────────────────────────────────────────────────────────────
# DATABASE SETTINGS
# ──────────────────────────────────────────────────────────────
POSTGRES_CONN_ID = "postgres_dw"
STAGING_TABLE = "stg_hashtag_raw"
DIM_HASHTAG_TABLE = "dim_hashtag"
DIM_DATE_TABLE = "dim_date"
FACT_TABLE = "fact_hashtag_daily"
RANKING_TABLE = "agg_hashtag_rank"

# ──────────────────────────────────────────────────────────────
# DATA QUALITY THRESHOLDS
# ──────────────────────────────────────────────────────────────
MIN_RECORDS_THRESHOLD = 1
MAX_NULL_PERCENTAGE = 0