ALTER TABLE bi_corp_staging.wahac690 ADD IF NOT EXISTS PARTITION (partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='previous_date_from', dag_id='LOAD_STG_MALHA-Daily') }}');
