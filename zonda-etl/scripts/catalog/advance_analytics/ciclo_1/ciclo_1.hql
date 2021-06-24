"set mapred.job.queue.name=root.dataeng;
SELECT nup,score
FROM analytics.ac_recu_predicciones where partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_process_date', dag_id='REPORTE_AA_CICLO_1') }}' "
