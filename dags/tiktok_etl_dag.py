import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))

"""
TikTok Hashtag ETL Pipeline - Main DAG
"""

# ──────────────────────────────────────────────────────────────
# AIRFLOW IMPORTS
# ──────────────────────────────────────────────────────────────
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime, timedelta
import logging

# ──────────────────────────────────────────────────────────────
# CONFIG IMPORTS
# ──────────────────────────────────────────────────────────────
from config.app_settings import (
    DAG_ID,
    DAG_DESCRIPTION,
    DAG_TAGS,
    DAG_OWNER,
    DAG_START_DATE,
    DAG_SCHEDULE,
    DAG_CATCHUP,
    DAG_MAX_ACTIVE_RUNS,
    TASK_RETRIES,
    TASK_RETRY_DELAY,
    TASK_EXECUTION_TIMEOUT,
    POSTGRES_CONN_ID,
)

# ──────────────────────────────────────────────────────────────
# MODULE IMPORTS
# ──────────────────────────────────────────────────────────────
from extractors.tiktok_api_extractor import TikTokAPIExtractor
from validators.data_quality_validator import DataQualityValidator
from utils.helpers import log_task_start, log_task_end, format_error_message

logger = logging.getLogger(__name__)

# ──────────────────────────────────────────────────────────────
# DEFAULT ARGS
# ──────────────────────────────────────────────────────────────
default_args = {
    "owner": DAG_OWNER,
    "retries": TASK_RETRIES,
    "retry_delay": TASK_RETRY_DELAY,
    "execution_timeout": TASK_EXECUTION_TIMEOUT,
    "on_failure_callback": lambda context: logger.error(
        format_error_message(Exception("DAG Failed"), context)
    ),
}

# ──────────────────────────────────────────────────────────────
# TASK FUNCTIONS
# ──────────────────────────────────────────────────────────────
def extract_task(**context):
    log_task_start("extract", context['ds'])
    extractor = TikTokAPIExtractor(postgres_conn_id=POSTGRES_CONN_ID)
    result = extractor.extract_and_load(report_date=context['ds'])
    log_task_end("extract", result)
    return result

def validate_task(**context):
    log_task_start("validate", context['ds'])
    validator = DataQualityValidator(postgres_conn_id=POSTGRES_CONN_ID)
    result = validator.validate(report_date=context['ds'])
    log_task_end("validate", result)
    return result

def load_sql(filename: str) -> str:
    import os
    dag_dir = Path(__file__).parent
    with open(os.path.join(dag_dir, 'sql', filename), 'r') as f:
        return f.read()

# ──────────────────────────────────────────────────────────────
# DAG DEFINITION
# ──────────────────────────────────────────────────────────────
with DAG(
    dag_id=DAG_ID,
    start_date=datetime.strptime(DAG_START_DATE, "%Y-%m-%d"),
    schedule=DAG_SCHEDULE,
    catchup=DAG_CATCHUP,
    default_args=default_args,
    description=DAG_DESCRIPTION,
    tags=DAG_TAGS,
    max_active_runs=DAG_MAX_ACTIVE_RUNS,
) as dag:

    clean_staging = PostgresOperator(
        task_id="clean_staging",
        postgres_conn_id=POSTGRES_CONN_ID,
        sql=load_sql('clean_staging.sql'),
    )

    extract = PythonOperator(
        task_id="mock_api_data",
        python_callable=extract_task,
        provide_context=True,
    )

    validate = PythonOperator(
        task_id="check_data_quality",
        python_callable=validate_task,
        provide_context=True,
    )

    load_dim_hashtag = PostgresOperator(
        task_id="load_dim_hashtag",
        postgres_conn_id=POSTGRES_CONN_ID,
        sql=load_sql('load_dim_hashtag.sql'),
    )

    transform_to_fact = PostgresOperator(
        task_id="transform_to_fact",
        postgres_conn_id=POSTGRES_CONN_ID,
        sql=load_sql('transform_to_fact.sql'),
    )

    build_hashtag_rank = PostgresOperator(
        task_id="build_hashtag_rank",
        postgres_conn_id=POSTGRES_CONN_ID,
        sql=load_sql('build_hashtag_rank.sql'),
    )

    clean_staging >> extract >> validate >> load_dim_hashtag >> transform_to_fact >> build_hashtag_rank