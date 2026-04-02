-- Load unique hashtags to dimension table
-- Smart category assignment based on hashtag name

INSERT INTO dim_hashtag (hashtag_name, category, created_at, is_active)
SELECT DISTINCT 
    hashtag_name,
    CASE 
        -- Technology
        WHEN hashtag_name IN ('#tech', '#ai', '#coding') THEN 'Technology'
        -- Business
        WHEN hashtag_name IN ('#marketing', '#entrepreneur', '#business') THEN 'Business'
        -- Entertainment
        WHEN hashtag_name IN ('#dance', '#music', '#comedy') THEN 'Entertainment'
        -- Lifestyle
        WHEN hashtag_name IN ('#travel', '#food', '#fashion') THEN 'Lifestyle'
        -- Education
        WHEN hashtag_name IN ('#learnontiktok') THEN 'Education'
        -- Health
        WHEN hashtag_name IN ('#fitness', '#health') THEN 'Health'
        ELSE 'General'
    END as category,
    CURRENT_TIMESTAMP as created_at,
    TRUE as is_active
FROM stg_hashtag_raw s
WHERE NOT EXISTS (
    SELECT 1 FROM dim_hashtag d WHERE d.hashtag_name = s.hashtag_name
)
ON CONFLICT (hashtag_name) DO NOTHING;