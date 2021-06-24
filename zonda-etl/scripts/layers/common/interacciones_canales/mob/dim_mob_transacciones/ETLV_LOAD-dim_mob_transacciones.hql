"
SET mapred.job.queue.name=root.dataeng;
SET hive.exec.dynamic.partition.mode=nonstrict;

INSERT OVERWRITE TABLE bi_corp_common.dim_mob_transacciones
PARTITION (partition_date)
SELECT
DISTINCT tr.transaccion																							cod_mob_transaccion,
trim(tr.descripcion) 																					ds_mob_descripcion,
trim(tr.modulo)																							ds_mob_modulo,
CASE WHEN trim(tr.habilitado)='1' THEN 1 ELSE 0 END                                                     flag_mob_habilitado,
partition_date
FROM bi_corp_staging.rio78_transaccion tr
WHERE tr.partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_MOB') }}'


"