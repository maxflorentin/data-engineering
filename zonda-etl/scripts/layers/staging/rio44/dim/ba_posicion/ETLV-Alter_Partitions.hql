ALTER TABLE bi_corp_staging.rio44_ba_posicion ADD IF NOT EXISTS PARTITION (partition_date='{partition_date}');