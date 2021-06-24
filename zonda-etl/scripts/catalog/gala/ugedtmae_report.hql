"SET mapred.job.queue.name=root.dataeng;
Select  *  from bi_corp_staging.ugdtmae
WHERE partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='previous_date_from', dag_id='GALA_UGDTMAE_REPORT') }}'
and cod_proceden in ('APQ', 'IPQ')
and Producto  = '35'
and SITPRES = '0'
"