ALTER TABLE bi_corp_staging.normalizacion ADD IF NOT EXISTS PARTITION (partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_STG_Garra_Normalizacion-Daily') }}');
