"set mapred.job.queue.name=root.dataeng;
SELECT cod_per_nup, id , seq_name
FROM bi_corp_staging.aaml_sancionados_novedades where partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='REPORTE_AA_SANCIONADOS') }}' "
