ALTER TABLE bi_corp_staging.rio102_mvw_prem_nov_costo_sorpresa ADD IF NOT EXISTS PARTITION (fecha_proc='{partition_date}');