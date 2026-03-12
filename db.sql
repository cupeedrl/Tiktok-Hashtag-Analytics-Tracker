
-- DATA WAREHOUSE: TIKTOK HASHTAG ANALYTICS

-- 1. STAGING TABLE (Raw Data Layer)
-- Lưu dữ liệu thô từ API, chưa qua xử lý
-- =============================================
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

-- =============================================
-- 2. DIMENSION TABLE: DATE (Core Layer)
-- Quản lý thông tin thời gian
-- =============================================
CREATE TABLE IF NOT EXISTS dim_date (
    date_id DATE PRIMARY KEY,
    day_of_week INT,
    day_name VARCHAR(10),
    month INT,
    month_name VARCHAR(10),
    quarter INT,
    year INT,
    is_weekend BOOLEAN
);

-- =============================================
-- 3. DIMENSION TABLE: HASHTAG (Core Layer)
-- Quản lý thông tin hashtag
-- =============================================
CREATE TABLE IF NOT EXISTS dim_hashtag (
    hashtag_id SERIAL PRIMARY KEY,
    hashtag_name VARCHAR(50) UNIQUE NOT NULL,
    category VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    is_active BOOLEAN DEFAULT TRUE
);

-- =============================================
-- 4. FACT TABLE: HASHTAG DAILY (Core Layer)
-- Trung tâm của Star Schema - Số liệu hàng ngày
-- =============================================
CREATE TABLE IF NOT EXISTS fact_hashtag_daily (
    fact_id SERIAL PRIMARY KEY,
    date_id DATE REFERENCES dim_date(date_id),
    hashtag_id INT REFERENCES dim_hashtag(hashtag_id),
    total_views INTEGER,
    total_likes INTEGER,
    total_shares INTEGER,
    total_comments INTEGER,
    engagement_rate DECIMAL(10, 4),
    UNIQUE(date_id, hashtag_id)
);

-- =============================================
-- 5. ANALYTICS TABLE: RANKING (Serving Layer)
-- Kết quả xếp hạng để Dashboard query nhanh
-- =============================================
CREATE TABLE IF NOT EXISTS agg_hashtag_rank (
    id SERIAL PRIMARY KEY,
    report_date DATE NOT NULL,
    hashtag VARCHAR(50) NOT NULL,
    total_views INTEGER,
    daily_rank INTEGER,
    wow_growth DECIMAL(10, 2),
    UNIQUE(report_date, hashtag)
);

-- =============================================
-- INSERT DỮ LIỆU MẪU CHO DIM_DATE (2020-2030)
-- =============================================
INSERT INTO dim_date (date_id, day_of_week, day_name, month, month_name, quarter, year, is_weekend)
SELECT 
    d::date as date_id,
    EXTRACT(DOW FROM d) as day_of_week,
    TO_CHAR(d, 'Day') as day_name,
    EXTRACT(MONTH FROM d) as month,
    TO_CHAR(d, 'Month') as month_name,
    EXTRACT(QUARTER FROM d) as quarter,
    EXTRACT(YEAR FROM d) as year,
    CASE WHEN EXTRACT(DOW FROM d) IN (0, 6) THEN TRUE ELSE FALSE END as is_weekend
FROM generate_series('2020-01-01'::date, '2030-12-31'::date, '1 day'::interval) d
ON CONFLICT (date_id) DO NOTHING;

-- =============================================
-- INSERT DỮ LIỆU MẪU CHO DIM_HASHTAG (6 Hashtags)
-- =============================================
INSERT INTO dim_hashtag (hashtag_name, category) VALUES 
('#marketing', 'Business'),
('#learnontiktok', 'Education'),
('#dance', 'Entertainment'),
('#food', 'Lifestyle'),
('#tech', 'Technology'),
('#travel', 'Lifestyle')
ON CONFLICT (hashtag_name) DO NOTHING;