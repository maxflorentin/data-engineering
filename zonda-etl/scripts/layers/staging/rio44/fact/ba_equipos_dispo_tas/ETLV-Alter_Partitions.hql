ALTER TABLE bi_corp_staging.rio44_ba_equipos_dispo_tas ADD IF NOT EXISTS PARTITION (partition_date='{partition_date}');