"""
Data Quality Validator Module
Validates data quality before transformation.
"""

import logging
from typing import Dict, Tuple
from airflow.exceptions import AirflowException
from airflow.hooks.postgres_hook import PostgresHook

logger = logging.getLogger(__name__)


class DataQualityValidator:
    """Validator class for data quality checks"""
    
    def __init__(self, postgres_conn_id: str = "postgres_dw"):
        self.hook = PostgresHook(postgres_conn_id=postgres_conn_id)
    
    def get_quality_checks(self, report_date: str) -> Dict[str, Tuple[str, tuple]]:
        """Define data quality checks"""
        return {
            "null_hashtag": (
                "SELECT COUNT(*) FROM stg_hashtag_raw WHERE hashtag IS NULL AND report_date = %s",
                (report_date,)
            ),
            "null_views": (
                "SELECT COUNT(*) FROM stg_hashtag_raw WHERE views IS NULL AND report_date = %s",
                (report_date,)
            ),
            "negative_views": (
                "SELECT COUNT(*) FROM stg_hashtag_raw WHERE views < 0 AND report_date = %s",
                (report_date,)
            ),
            "negative_engagement": (
                "SELECT COUNT(*) FROM stg_hashtag_raw WHERE engagement_rate < 0 AND report_date = %s",
                (report_date,)
            ),
            "min_records": (
                "SELECT COUNT(*) FROM stg_hashtag_raw WHERE report_date = %s",
                (report_date,)
            ),
        }
    
    def validate(self, report_date: str, min_records: int = 1) -> str:
        """Run all data quality checks"""
        logger.info(f"Running data quality checks for date: {report_date}")
        
        checks = self.get_quality_checks(report_date)
        
        for check_name, (query, params) in checks.items():
            result = self.hook.get_first(query, parameters=params)[0]
            
            if check_name == "min_records":
                if result < min_records:
                    raise AirflowException(
                        f"Data quality check failed: {check_name} = {result} (minimum {min_records} required)"
                    )
            else:
                if result > 0:
                    raise AirflowException(
                        f"Data quality check failed: {check_name} = {result} (should be 0)"
                    )
            
            logger.info(f"✓ {check_name}: {result} (OK)")
        
        logger.info("✅ All data quality checks passed")
        return "Data quality verified"