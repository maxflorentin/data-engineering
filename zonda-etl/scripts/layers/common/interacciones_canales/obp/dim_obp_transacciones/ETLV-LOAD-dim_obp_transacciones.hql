"
SET hive.exec.dynamic.partition.mode=nonstrict;
SET mapred.job.queue.name=root.dataeng;

INSERT OVERWRITE TABLE bi_corp_common.dim_obp_transacciones
PARTITION (partition_date)
select
trim(transaccion)                                                       cod_obp_transaccion,
trim(descripcion)                                                       ds_obp_descripcion,
partition_date                                                          partition_date
from bi_corp_staging.rio147_hb_desc_trx
where partition_date="{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_OBP-Daily') }}"

"