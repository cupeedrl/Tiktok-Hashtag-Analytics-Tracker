-- Load unique hashtags to dimension table
-- Smart category assignment based on hashtag name

INSERT INTO dim_hashtag (hashtag, category, created_at, is_active)
SELECT DISTINCT 
    hashtag,
    CASE 
        WHEN hashtag IN ('#tech', '#learnontiktok') THEN 'Technology'
        WHEN hashtag IN ('#marketing') THEN 'Business'
        WHEN hashtag IN ('#dance', '#food', '#travel') THEN 'Lifestyle'
        ELSE 'General'
    END as category,
    CURRENT_TIMESTAMP as created_at,
    TRUE as is_active
FROM stg_hashtag_raw s
WHERE NOT EXISTS (
    SELECT 1 FROM dim_hashtag d WHERE d.hashtag = s.hashtag
)
ON CONFLICT (hashtag) DO NOTHING;