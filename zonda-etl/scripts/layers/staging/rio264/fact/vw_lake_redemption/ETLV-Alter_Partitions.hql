ALTER TABLE bi_corp_staging.fm_rescates ADD IF NOT EXISTS PARTITION (partition_date='{partition_date}');