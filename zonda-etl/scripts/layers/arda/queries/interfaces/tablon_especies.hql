"SET mapred.job.queue.name=root.dataeng;
SELECT * FROM SANTANDER_BUSINESS_RISK_ARDA.ESPECIES WHERE DATA_DATE_PART = '{{ ti.xcom_pull(task_ids='PREVIOUS_WORKING_DAY', key='date_previous_working_day', dag_id='ARDA_Loader_Tablon_Especies') }}';"