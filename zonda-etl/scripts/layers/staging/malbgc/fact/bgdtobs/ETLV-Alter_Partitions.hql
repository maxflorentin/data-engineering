ALTER TABLE bi_corp_staging.malbgc_bgdtobs ADD IF NOT EXISTS PARTITION (partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_STG_MALBGC_Trx-Daily') }}');