"SET mapred.job.queue.name=root.dataeng;

SELECT *
   FROM  bi_corp_common.stk_gar_garantias
WHERE partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='REPORT_SANTANDER_CONSUMER') }}'
"