ALTER TABLE bi_corp_staging.cacs_movimientos ADD IF NOT EXISTS PARTITION (partition_date='{partition_date}');