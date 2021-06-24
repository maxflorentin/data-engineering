ALTER TABLE bi_corp_staging.malug_ugdtdrb ADD IF NOT EXISTS PARTITION (partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_STG_MALUG-Daily') }}');
