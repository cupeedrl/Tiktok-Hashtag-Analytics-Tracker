-- TIKTOK HASHTAG DATA WAREHOUSE

DROP TABLE IF EXISTS fact_hashtag_daily CASCADE;
DROP TABLE IF EXISTS agg_hashtag_rank CASCADE;
DROP TABLE IF EXISTS dim_hashtag CASCADE;
DROP TABLE IF EXISTS dim_date CASCADE;
DROP TABLE IF EXISTS stg_hashtag_raw CASCADE;

-- TAGING TABLE (RAW DATA - MINIMAL LOGIC)
CREATE TABLE stg_hashtag_raw (
    id SERIAL PRIMARY KEY,
    hashtag_name VARCHAR(255) NOT NULL,
    report_date DATE NOT NULL,
    views BIGINT,
    likes BIGINT,
    shares BIGINT,
    comments BIGINT,
    engagement_rate DECIMAL(10,4),
    extracted_at TIMESTAMP,
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(hashtag_name, report_date)
);

CREATE INDEX idx_stg_report_date ON stg_hashtag_raw(report_date);
CREATE INDEX idx_stg_hashtag_name ON stg_hashtag_raw(hashtag_name);

-- DIMENSION TABLES

CREATE TABLE dim_date (
    date_id DATE PRIMARY KEY,
    year INT,
    month INT,
    day INT,
    quarter INT,
    week_of_year INT,
    day_of_week INT,
    is_weekend BOOLEAN
);

CREATE INDEX idx_dim_date_year ON dim_date(year);
CREATE INDEX idx_dim_date_month ON dim_date(month);

-- HASHTAG DIMENSION
CREATE TABLE dim_hashtag (
    hashtag_id SERIAL PRIMARY KEY,
    hashtag_name VARCHAR(255) UNIQUE NOT NULL,
    category VARCHAR(100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    is_active BOOLEAN DEFAULT TRUE
);

CREATE INDEX idx_dim_hashtag_name ON dim_hashtag(hashtag_name);


-- FACT TABLE 

CREATE TABLE fact_hashtag_daily (
    fact_id SERIAL PRIMARY KEY,
    hashtag_id INT NOT NULL REFERENCES dim_hashtag(hashtag_id),
    date_id DATE NOT NULL REFERENCES dim_date(date_id),

    views BIGINT CHECK (views >= 0),
    likes BIGINT CHECK (likes >= 0),
    shares BIGINT CHECK (shares >= 0),
    comments BIGINT CHECK (comments >= 0),

    engagement_rate DECIMAL(10,4) CHECK (engagement_rate BETWEEN 0 AND 1),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(date_id, hashtag_id)
);

CREATE INDEX idx_fact_hashtag_id ON fact_hashtag_daily(hashtag_id);
CREATE INDEX idx_fact_date_id ON fact_hashtag_daily(date_id);

-- AGG TABLE 
CREATE TABLE agg_hashtag_rank (
    rank_id SERIAL PRIMARY KEY,
    report_date DATE NOT NULL,
    hashtag_id INT NOT NULL REFERENCES dim_hashtag(hashtag_id),
    total_views BIGINT CHECK (total_views >= 0),
    rank_by_views INT,

    UNIQUE(report_date, hashtag_id)
);

CREATE INDEX idx_agg_report_date ON agg_hashtag_rank(report_date);
CREATE INDEX idx_agg_hashtag_id ON agg_hashtag_rank(hashtag_id);

-- LOAD DIM_DATE
INSERT INTO dim_date (
    date_id,
    year,
    month,
    day,
    quarter,
    week_of_year,
    day_of_week,
    is_weekend
)
SELECT 
    d::date,
    EXTRACT(YEAR FROM d)::int,
    EXTRACT(MONTH FROM d)::int,
    EXTRACT(DAY FROM d)::int,
    EXTRACT(QUARTER FROM d)::int,
    EXTRACT(WEEK FROM d)::int,
    EXTRACT(DOW FROM d)::int,
    (EXTRACT(DOW FROM d) IN (0,6))
FROM generate_series(
    '2020-01-01'::date,
    '2030-12-31'::date,
    '1 day'::interval
) d
ON CONFLICT (date_id) DO NOTHING;


-- LOAD DIM_HASHTAG
INSERT INTO dim_hashtag (hashtag_name, category) VALUES 
-- Technology 
('#tech', 'Technology'),
('#ai', 'Technology'),
('#coding', 'Technology'),
-- Business 
('#marketing', 'Business'),
('#entrepreneur', 'Business'),
('#business', 'Business'),
-- Entertainment 
('#dance', 'Entertainment'),
('#music', 'Entertainment'),
('#comedy', 'Entertainment'),
-- Lifestyle 
('#travel', 'Lifestyle'),
('#food', 'Lifestyle'),
('#fashion', 'Lifestyle'),
-- Education 
('#learnontiktok', 'Education'),
-- Health 
('#fitness', 'Health'),
('#health', 'Health')
ON CONFLICT (hashtag_name) DO NOTHING;

-- CHECK DATA
SELECT 'stg_hashtag_raw' AS table_name, COUNT(*) FROM stg_hashtag_raw
UNION ALL
SELECT 'dim_hashtag', COUNT(*) FROM dim_hashtag
UNION ALL
SELECT 'dim_date', COUNT(*) FROM dim_date
UNION ALL
SELECT 'fact_hashtag_daily', COUNT(*) FROM fact_hashtag_daily
UNION ALL
SELECT 'agg_hashtag_rank', COUNT(*) FROM agg_hashtag_rank
ORDER BY table_name;