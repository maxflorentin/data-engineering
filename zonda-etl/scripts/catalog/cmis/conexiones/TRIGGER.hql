"SET mapred.job.queue.name=root.dataeng;
select 'AR_BSPL1_{{ ti.xcom_pull(task_ids='InputConfig', key='previous_month-1_date_to_arda', dag_id='CMIS_TRIGGER') }}.csv' nombre
UNION ALL select 'AR_BSPL2_{{ ti.xcom_pull(task_ids='InputConfig', key='previous_month-1_date_to_arda', dag_id='CMIS_TRIGGER') }}.csv' nombre
UNION ALL select 'AR_BIND_{{ ti.xcom_pull(task_ids='InputConfig', key='previous_month-1_date_to_arda', dag_id='CMIS_TRIGGER') }}.csv' nombre
UNION ALL select 'AR_CAT_{{ ti.xcom_pull(task_ids='InputConfig', key='previous_month-1_date_to_arda', dag_id='CMIS_TRIGGER') }}.csv' nombre
UNION ALL select 'AR_OF1_{{ ti.xcom_pull(task_ids='InputConfig', key='previous_month-1_date_to_arda', dag_id='CMIS_TRIGGER') }}.csv' nombre
UNION ALL select 'AR_RAT1_{{ ti.xcom_pull(task_ids='InputConfig', key='previous_month-1_date_to_arda', dag_id='CMIS_TRIGGER') }}.csv' nombre
UNION ALL select 'AR_RAT2_{{ ti.xcom_pull(task_ids='InputConfig', key='previous_month-1_date_to_arda', dag_id='CMIS_TRIGGER') }}.csv' nombre
UNION ALL select 'AR_RWA1_{{ ti.xcom_pull(task_ids='InputConfig', key='previous_month-1_date_to_arda', dag_id='CMIS_TRIGGER') }}.csv' nombre
UNION ALL select 'AR_RWA2_{{ ti.xcom_pull(task_ids='InputConfig', key='previous_month-1_date_to_arda', dag_id='CMIS_TRIGGER') }}.csv' nombre"