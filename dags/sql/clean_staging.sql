-- Clean staging table for execution date
-- Removes old data before new insert (idempotency)

DELETE FROM stg_hashtag_raw
WHERE report_date = '{{ ds }}';