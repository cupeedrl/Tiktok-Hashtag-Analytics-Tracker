"""
TikTok API Extractor Module
Handles data extraction from TikTok Mock API.
"""

import logging
from typing import List, Dict, Any
from airflow.exceptions import AirflowException
from airflow.hooks.postgres_hook import PostgresHook
from scripts.mock_api import TikTokMockAPI
from psycopg2.extras import execute_values
logger = logging.getLogger(__name__)


class TikTokAPIExtractor:
    """
    Extractor class for TikTok hashtag data.
    """
    
    def __init__(self, postgres_conn_id: str = "postgres_dw"):
        self.postgres_conn_id = postgres_conn_id
        self.hook = PostgresHook(postgres_conn_id=postgres_conn_id)
    
    def fetch_data(self, report_date: str) -> List[Dict[str, Any]]:
        """Fetch data from Mock TikTok API"""
        logger.info(f"Fetching data for date: {report_date}")
        
        api = TikTokMockAPI()
        data = api.fetch_all_hashtags(report_date)
        
        if not data:
            raise AirflowException(f"No data fetched for date: {report_date}")
        
        logger.info(f"Fetched {len(data)} records from mock API")
        return data
    
    def prepare_records(self, data: List[Dict[str, Any]]) -> List[tuple]:
        """Prepare data for bulk insert"""
        return [
            (
                row["hashtag"], row["report_date"], row["views"],
                row["likes"], row["shares"], row["comments"],
                row["engagement_rate"], row["extracted_at"]
            )
            for row in data
        ]
    
    def bulk_insert(self, records: List[tuple], report_date: str) -> int:
        """True bulk insert with UPSERT using execute_values"""
        logger.info(f"Bulk inserting {len(records)} records with UPSERT")
        
        conn = self.hook.get_conn()
        cur = conn.cursor()
        
        try:
            # Single SQL statement with multiple VALUES tuples
            execute_values(
                cur,
                """
                INSERT INTO stg_hashtag_raw 
                (hashtag_name, report_date, views, likes, shares, comments, engagement_rate, extracted_at)
                VALUES %s
                ON CONFLICT (hashtag_name, report_date) 
                DO UPDATE SET
                    views = EXCLUDED.views,
                    likes = EXCLUDED.likes,
                    shares = EXCLUDED.shares,
                    comments = EXCLUDED.comments,
                    engagement_rate = EXCLUDED.engagement_rate,
                    extracted_at = EXCLUDED.extracted_at
                """,
                records,  # List of tuples
                page_size=100  # Batch size for very large lists
            )
            
            conn.commit()
            logger.info(f"Successfully bulk upserted {len(records)} records")
            return len(records)
        
        except Exception as e:
            conn.rollback()
            logger.error(f"Error during bulk insert: {e}")
            raise
        
        finally:
            cur.close()
            conn.close()
    
    def extract_and_load(self, report_date: str) -> str:
        """Main extraction method"""
        data = self.fetch_data(report_date)
        records = self.prepare_records(data)
        count = self.bulk_insert(records, report_date)
        return f"Inserted {count} records for {report_date}"
