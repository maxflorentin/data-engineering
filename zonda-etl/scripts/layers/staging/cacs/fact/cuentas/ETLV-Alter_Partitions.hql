ALTER TABLE bi_corp_staging.cacs_cuentas ADD IF NOT EXISTS PARTITION (partition_date='{partition_date}');