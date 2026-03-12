import psycopg2
from psycopg2.extras import execute_values
import os
from dotenv import load_dotenv
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Load biến môi trường từ file .env
load_dotenv()

def get_postgres_connection():
    """
    # connect PostgreSQL Data Warehouse.
    """
    conn = psycopg2.connect(
        host=os.getenv('POSTGRES_HOST', 'postgres_dw'),  # Tên service trong docker-compose
        port=os.getenv('POSTGRES_PORT', '5432'),
        database=os.getenv('POSTGRES_DB', 'tiktok_dw'),
        user=os.getenv('POSTGRES_USER', 'postgres'),
        password=os.getenv('POSTGRES_PASSWORD', 'postgres')
    )
    return conn

def create_staging_table():

    conn = get_postgres_connection()
    cur = conn.cursor()
    
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS stg_hashtag_raw (
        id SERIAL PRIMARY KEY,
        hashtag VARCHAR(50) NOT NULL,
        report_date DATE NOT NULL,
        views INTEGER,
        likes INTEGER,
        shares INTEGER,
        comments INTEGER,
        engagement_rate DECIMAL(10, 4),
        extracted_at TIMESTAMP,
        loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """
    
    try:
        cur.execute(create_table_sql)
        conn.commit()
        logger.info("Staging table created successfully")
    except Exception as e:
        logger.error(f"Error creating table: {e}")
        conn.rollback()
    finally:
        cur.close()
        conn.close()

def insert_to_staging(data: list):
   
    if not data:
        logger.warning("No data to insert")
        return
    
    conn = get_postgres_connection()
    cur = conn.cursor()
    
    insert_sql = """
    INSERT INTO stg_hashtag_raw 
    (hashtag, report_date, views, likes, shares, comments, engagement_rate, extracted_at)
    VALUES %s
    """
    
    values = [
        (
            item['hashtag'],
            item['report_date'],
            item['views'],
            item['likes'],
            item['shares'],
            item['comments'],
            item['engagement_rate'],
            item['extracted_at']
        )
        for item in data
    ]
    
    try:
        execute_values(cur, insert_sql, values)
        conn.commit()
        logger.info(f"Inserted {len(values)} records to staging")
    except Exception as e:
        logger.error(f"Error inserting data: {e}")
        conn.rollback()
    finally:
        cur.close()
        conn.close()

if __name__ == "__main__":
    create_staging_table()
    print("Database connection test completed!")