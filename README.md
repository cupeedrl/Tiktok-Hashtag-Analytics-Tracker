# Tiktok-Hashtag-Analytics-Tracker
End-to-end Data Pipeline for TikTok Hashtag Analytics using Apache Airflow, PostgreSQL, and Metabase

## 📋 Executive Summary
A production-grade Data Engineering pipeline that processes social media analytics data from extraction through visualization. Built to demonstrate industry-standard practices in ETL orchestration, data warehousing, and business intelligence.  
- Pipeline Tasks: 6 orchestrated tasks with retry logic  
- Data Quality Checks: 5 validation rules  
- Data Tables: 5 tables (Star Schema)  
- Schedule: Daily automated execution (@daily)  
- Backfill Support: Safe for historical data processing  
- Date Range: 4,018 days (2020-2030)  
## 🎯 Business Problem
Marketing teams need real-time visibility into hashtag performance to:  
- Identify trending topics before competitors  
- Optimize content strategy based on engagement metrics  
- Track campaign performance across multiple hashtags  
- Make data-driven decisions on ad spend allocation  
This pipeline solves that by providing:  
- Automated daily data collection  
- Clean, analytics-ready data warehouse  
- Interactive dashboards for stakeholder consumption
- Data quality enforcement at every stage  
## Infrastructure Components

| Component         | Technology           | Purpose                                                  |
|-------------------|----------------------|----------------------------------------------------------|
| Orchestration     | Apache Airflow 2.8.0 | Pipeline scheduling, monitoring, retry logic             |
| Data Warehouse    | PostgreSQL 15        | Star schema storage with referential integrity           |
| BI & Analytics    | Metabase             | Self-service dashboards for business users               |
| Containerization  | Docker Compose       | Reproducible environments, easy deployment               |
| Language          | Python 3.10          | ETL logic, API simulation, data transformations          |

## Technical Architecture:
<img width="1362" height="722" alt="image" src="https://github.com/user-attachments/assets/92ddf58c-d721-4673-9d93-65868d6c0095" />


## Infrastructure Components  

| Component         | Technology           | Purpose                                                  |
|-------------------|----------------------|----------------------------------------------------------|
| Orchestration     | Apache Airflow 2.8.0 | Pipeline scheduling, monitoring, retry logic             |
| Data Warehouse    | PostgreSQL 15        | Star schema storage with referential integrity           |
| BI & Analytics    | Metabase             | Self-service dashboards for business users               |
| Containerization  | Docker Compose       | Reproducible environments, easy deployment               |
| Language          | Python 3.10          | ETL logic, API simulation, data transformations          |


## Key Features
### Data Engineering Best Practices

| Feature           | Implementation                                      | Business Value                                     |
|-------------------|-----------------------------------------------------|----------------------------------------------------|
| Idempotency       | UPSERT logic, date-based deduplication              | Safe re-runs without data duplication              |
| Data Quality      | 5 validation checks (NULL, negative values, min records) | Ensures data integrity at ingestion            |
| Error Handling    | 2 retries with 30s delay, 10min timeout             | Pipeline resilience against transient failures     |
| Incremental Load  | Date-partitioned processing with `{{ ds }}`         | Efficient resource utilization                     |
| Monitoring        | Health checks, comprehensive logging                | Proactive issue detection                          |
| Backfill Support  | Uses `execution_date` not `datetime.now()`          | Safe historical data processing                    |
## Data Modeling

Dimensional Modeling (Star Schema)

            dim_date (4,018 rows)
                       │
                       │
        ┌──────────────┼──────────────┐
        │              │              │
        ▼              ▼              ▼
       dim_hashtag    fact_hashtag    agg_hashtag_rank
       (6 rows)       daily (6/day)   (6/day)

| Table               | Type       | Purpose                              | Growth           |
|---------------------|------------|--------------------------------------|------------------|
| stg_hashtag_raw     | Staging    | Raw API data, audit trail            | ~30 rows/day     |
| dim_date            | Dimension  | Time intelligence, no ETL            | Static (4,018)   |
| dim_hashtag         | Dimension  | Hashtag master data                  | ~6 rows          |
| fact_hashtag_daily  | Fact       | Grain: hashtag × day                 | ~6 rows/day      |
| agg_hashtag_rank    | Analytics  | Pre-computed rankings                | ~6 rows/day      |


## Dag flow
clean_staging → mock_api_data → check_data_quality → load_dim_hashtag → transform_to_fact → build_hashtag_rank
| Task ID            | Type             | Description                                   |
|--------------------|------------------|-----------------------------------------------|
| clean_staging      | PostgresOperator | Delete old data for execution date            |
| mock_api_data      | PythonOperator   | Extract data from mock API (bulk insert)      |
| check_data_quality | PythonOperator   | Validate data (5 checks)                      |
| load_dim_hashtag   | PostgresOperator | Load unique hashtags to dimension             |
| transform_to_fact  | PostgresOperator | Aggregate staging → fact table                |
| build_hashtag_rank | PostgresOperator | Calculate daily rankings                      |
## Quick Start
### Required Software
Docker Desktop 4.0+     # https://docker.com  
Python 3.8+             # https://python.org  
4GB RAM minimum         # 8GB recommended  
5GB available disk      # For containers + data  

### Installation
1. Clone repository
git clone https://github.com/cupeedrl/tiktok-hashtag-analytics.git
cd tiktok-analytics-de

2. Configure environment
cp .env.example .env

3. Start all services
docker-compose up -d

4. Wait for initialization (90 seconds)
timeout /t 90

5. Verify health status
docker-compose ps  

### Expected Ouput:   

| NAME               | STATUS        | PORTS                   |
|--------------------|---------------|-------------------------|
| postgres_dw        | Up (healthy)  | 0.0.0.0:5433->5432/tcp  |
| airflow-webserver  | Up (healthy)  | 0.0.0.0:8080->8080/tcp  |
| airflow-scheduler  | Up (healthy)  | -                       |
| metabase           | Up (healthy)  | 0.0.0.0:3000->3000/tcp  |

### Initialize Database  
```cmd
# Create schema and seed reference data
Get-Content db.sql | docker-compose exec -T postgres_dw psql -U postgres -d tiktok_dw
```

### Access Points
-Airflow (Pipeline monitoring): http://localhost:8080 
-Metabase (Business dashboards): http://localhost:3000  
-PostgreSQL (Direct SQL access): localhost: 5433  

### Execute Pipeline
-Navigate to Airflow UI (http://localhost:8080)  
-Enable tiktok_etl_dag toggle  
-Trigger manual run (Play button ▶)  
-Monitor task completion (~2-3 minutes)  
-Verify all tasks show success status  ✅
### Sample queries  
- Top Performing Hashtags:  
```sql  
SELECT 
    hashtag,
    total_views,
    daily_rank,
    wow_growth
FROM agg_hashtag_rank
WHERE report_date = CURRENT_DATE
ORDER BY daily_rank ASC
LIMIT 10;
```
- Engagement Rate Trend:  
```sql  
  SELECT 
    f.date_id,
    h.hashtag_name,
    AVG(f.engagement_rate) as avg_engagement,
    SUM(f.total_views) as total_views
FROM fact_hashtag_daily f
JOIN dim_hashtag h ON f.hashtag_id = h.hashtag_id
JOIN dim_date d ON f.date_id = d.date_id
WHERE d.year = 2026
GROUP BY f.date_id, h.hashtag_name
ORDER BY f.date_id DESC;
```  
- Week-over-Week Growth:  
```sql  
WITH current_week AS (
    SELECT hashtag, SUM(total_views) as views
    FROM agg_hashtag_rank
    WHERE report_date >= CURRENT_DATE - INTERVAL '7 days'
    GROUP BY hashtag
),
previous_week AS (
    SELECT hashtag, SUM(total_views) as views
    FROM agg_hashtag_rank
    WHERE report_date BETWEEN CURRENT_DATE - INTERVAL '14 days' 
                          AND CURRENT_DATE - INTERVAL '7 days'
    GROUP BY hashtag
)
SELECT 
    c.hashtag,
    c.views as current_week,
    p.views as previous_week,
    ROUND((c.views - p.views) * 100.0 / NULLIF(p.views, 0), 2) as growth_percent
FROM current_week c
LEFT JOIN previous_week p ON c.hashtag = p.hashtag
ORDER BY growth_percent DESC NULLS LAST;  
```
## 🔧 Technical Highlights  

1. Execution_date
-Problem: Using datetime.now() breaks backfill operations.  
-Solution: Use Airflow's {{ ds }} template variable:  
```sql
WHERE report_date = '{{ ds }}'  -- Uses execution_date, not current date
```
Result: Safe to backfill historical data without processing wrong dates.  
2. Bulk Insert for Performance  
-Problem: Row-by-row insert is slow for large datasets.  
-Solution: Use hook.insert_rows() with list of tuples:  
```sql
hook.insert_rows(
    table='stg_hashtag_raw',
    rows=records,  # List of tuples
    target_fields=[...],
    commit=True
)
```
3. Data Quality Enforcement    
-Problem: Bad data propagates through pipeline.  
-Solution: 5 validation checks before transformation:  
```python
checks = {
    "null_hashtag": "SELECT COUNT(*) ... WHERE hashtag IS NULL",
    "null_views": "SELECT COUNT(*) ... WHERE views IS NULL",
    "negative_views": "SELECT COUNT(*) ... WHERE views < 0",
    "negative_engagement": "SELECT COUNT(*) ... WHERE engagement_rate < 0",
    "min_records": "SELECT COUNT(*) ... WHERE report_date = %s",
}
```
4. Star Schema Compliance  
Problem: Direct date insert violates foreign key constraints.  
Solution: INNER JOIN with dim_date:  
```sql
INNER JOIN dim_date dd ON s.report_date::date = dd.date_id
```
5. Modular Code Architecture  
Problem: Monolithic DAGs are hard to maintain and test.  
Solution: Separate modules for extractors, validators, SQL, config:  
Result: Pipeline fails fast on bad data, preventing corruption.  
    dags/    
    ├── extractors/tiktok_api_extractor.py  
    ├── validators/data_quality_validator.py  
    ├── sql/*.sql  
    └── config/settings.py  
   
## Skills Demonstrated
- Data Orchestration: Apache Airflow, DAG design, Task dependencies  
- Data Warehousing: PostgreSQL, Star Schema, Dimensional modeling  
- ETL Development: Python, SQL, Bulk insert, UPSERT, Idempotency  
- Data Quality: Validation checks, NULL detection, Error handling  
- Containerization: Docker, Docker Compose, Health checks
- BI & Visualization: Metabase, SQL queries, Dashboard design  
- Code Quality: Modular architecture, Type hints, Documentation  
- Problem Solving: Timezone fix, PID cleanup, Backfill support
## Production Recommendations

1. Generate secure Fernet Key
python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"

2. Update .env with secure values
FERNET_KEY=<generated_key>
POSTGRES_PASSWORD=<secure_password>
AIRFLOW_PASSWORD=<secure_password>

3. Enable SSL for database connections
4. Implement secrets management (AWS Secrets Manager, HashiCorp Vault)
5. Add network policies and firewall rules

## 👤 Author: Dat Chu Quoc 

🔗 GitHub: https://github.com/cupeedrl

📧 Gmail: whisperkuu.41@gmail.com

💼 LinkedIn: https://www.linkedin.com/in/dat-chu-quoc-583599387/

📄 MIT License - Feel free to use for learning and portfolio purposes!


Last Updated: March 2026
