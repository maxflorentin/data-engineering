ALTER TABLE bi_corp_staging.academia_view_users_data ADD IF NOT EXISTS PARTITION (partition_date='{partition_date}');