ALTER TABLE bi_corp_staging.bgdtmso ADD IF NOT EXISTS PARTITION (partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_STG_MALBGC-Daily') }}');