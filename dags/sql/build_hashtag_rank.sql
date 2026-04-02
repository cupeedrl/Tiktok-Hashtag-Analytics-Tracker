-- Build daily hashtag rankings
-- Calculates rank based on total views
-- Only processes data for execution date

-- Clean old data for idempotency
DELETE FROM agg_hashtag_rank
WHERE report_date = '{{ ds }}';

-- Insert new rankings with UPSERT
INSERT INTO agg_hashtag_rank
(
    report_date,
    hashtag_id,
    total_views,
    rank_by_views
)
SELECT
    f.date_id::date as report_date,
    h.hashtag_id as hashtag,
    f.views,                       
    RANK() OVER (PARTITION BY f.date_id ORDER BY f.views DESC) as rank_by_views
FROM fact_hashtag_daily f
INNER JOIN dim_hashtag h ON f.hashtag_id = h.hashtag_id
WHERE f.date_id = '{{ ds }}'
ON CONFLICT (report_date, hashtag_id)
DO UPDATE SET
    total_views = EXCLUDED.total_views,
    rank_by_views = EXCLUDED.rank_by_views;