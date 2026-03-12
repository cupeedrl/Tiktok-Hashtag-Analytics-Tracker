from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.hooks.postgres_hook import PostgresHook
from datetime import datetime, timedelta


default_args = {
    "owner": "dat",
    "retries": 2,
    "retry_delay": timedelta(seconds=10)
}

def mock_tiktok_api(**context):
    """
    Task 1: Lấy data từ TikTokMockAPI và insert vào stg_hashtag_raw
    """
    from mock_api import TikTokMockAPI

    api = TikTokMockAPI()

    
    today = datetime.utcnow().date()

    data = api.fetch_all_hashtags(today)

    rows = []
    for row in data:
        rows.append(f"""(
            '{row["hashtag"]}',
            '{row["report_date"]}',
            {row["views"]},
            {row["likes"]},
            {row["shares"]},
            {row["comments"]},
            {row["engagement_rate"]},
            '{row["extracted_at"]}'
        )""")

    values_str = ",\n".join(rows)

    insert_sql = f"""
        INSERT INTO stg_hashtag_raw 
        (hashtag, report_date, views, likes, shares, comments, engagement_rate, extracted_at)
        VALUES {values_str};
    """

    postgres_hook = PostgresHook(postgres_conn_id="postgres_dw")
    postgres_hook.run(insert_sql)

    return f"Inserted {len(data)} records into stg_hashtag_raw"


with DAG(
    dag_id="tiktok_etl_dag",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args=default_args,
    description="TikTok Hashtag Data Warehouse ETL",
    tags=["tiktok", "etl", "social-media"],
    max_active_runs=1
) as dag:

    clean_staging = PostgresOperator(
        task_id="clean_staging",
        postgres_conn_id="postgres_dw",
        sql="""
        DELETE FROM stg_hashtag_raw
        WHERE report_date = CURRENT_DATE;
        """
    )

    mock_api = PythonOperator(
        task_id="mock_api_data",
        python_callable=mock_tiktok_api
    )

    load_dim_hashtag = PostgresOperator(
        task_id="load_dim_hashtag",
        postgres_conn_id="postgres_dw",
        sql="""
        INSERT INTO dim_hashtag (hashtag_name, category, created_at, is_active)
        SELECT DISTINCT 
            hashtag,
            'Technology' as category,
            CURRENT_TIMESTAMP as created_at,
            TRUE as is_active
        FROM stg_hashtag_raw s
        WHERE NOT EXISTS (
            SELECT 1
            FROM dim_hashtag d
            WHERE d.hashtag_name = s.hashtag
        )
        ON CONFLICT (hashtag_name) DO NOTHING;
        """
    )

    transform_to_fact = PostgresOperator(
        task_id="transform_to_fact",
        postgres_conn_id="postgres_dw",
        sql="""
        INSERT INTO fact_hashtag_daily
        (
            date_id,
            hashtag_id,
            total_views,
            total_likes,
            total_shares,
            total_comments,
            engagement_rate
        )
        SELECT
            s.report_date::date as date_id,
            h.hashtag_id,
            SUM(s.views) as total_views,
            SUM(s.likes) as total_likes,
            SUM(s.shares) as total_shares,
            SUM(s.comments) as total_comments,
            AVG(s.engagement_rate) as engagement_rate
        FROM stg_hashtag_raw s
        JOIN dim_hashtag h ON s.hashtag = h.hashtag_name
        GROUP BY s.report_date, h.hashtag_id
        ON CONFLICT (date_id, hashtag_id)
        DO UPDATE SET
            total_views = EXCLUDED.total_views,
            total_likes = EXCLUDED.total_likes,
            total_shares = EXCLUDED.total_shares,
            total_comments = EXCLUDED.total_comments,
            engagement_rate = EXCLUDED.engagement_rate;
        """
    )

  
    build_hashtag_rank = PostgresOperator(
        task_id="build_hashtag_rank",
        postgres_conn_id="postgres_dw",
        sql="""
        DELETE FROM agg_hashtag_rank;

        INSERT INTO agg_hashtag_rank
        (
            report_date,
            hashtag,
            total_views,
            daily_rank,
            wow_growth
        )
        SELECT
            f.date_id::date as report_date,
            h.hashtag_name as hashtag,
            f.total_views,
            RANK() OVER (PARTITION BY f.date_id ORDER BY f.total_views DESC) as daily_rank,
            NULL as wow_growth
        FROM fact_hashtag_daily f
        JOIN dim_hashtag h ON f.hashtag_id = h.hashtag_id;
        """
    )

    clean_staging >> mock_api >> load_dim_hashtag >> transform_to_fact >> build_hashtag_rank