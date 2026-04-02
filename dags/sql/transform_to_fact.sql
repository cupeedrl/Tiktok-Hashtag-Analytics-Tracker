INSERT INTO fact_hashtag_daily
(
    date_id,
    hashtag_id,
    views,
    likes,
    shares,
    comments,
    engagement_rate
)
SELECT
    dd.date_id as date_id,
    h.hashtag_id,
    SUM(s.views) as views,
    SUM(s.likes) as likes,
    SUM(s.shares) as shares,
    SUM(s.comments) as comments,
    AVG(s.engagement_rate) as engagement_rate
FROM stg_hashtag_raw s
INNER JOIN dim_hashtag h ON s.hashtag_name = h.hashtag_name
INNER JOIN dim_date dd ON s.report_date::date = dd.date_id
WHERE s.report_date = '{{ ds }}'    
GROUP BY dd.date_id, h.hashtag_id
ON CONFLICT (date_id, hashtag_id)
DO UPDATE SET
    views = EXCLUDED.views,
    likes = EXCLUDED.likes,
    shares = EXCLUDED.shares,
    comments = EXCLUDED.comments,
    engagement_rate = EXCLUDED.engagement_rate;