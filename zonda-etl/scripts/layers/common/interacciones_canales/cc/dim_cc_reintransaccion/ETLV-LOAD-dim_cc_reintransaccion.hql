SET mapred.job.queue.name=root.dataeng;
SET hive.exec.dynamic.partition.mode=nonstrict;

INSERT OVERWRITE TABLE bi_corp_common.dim_cc_reintransacciones
PARTITION(partition_date)

SELECT
DISTINCT trim(nombre)                                	COD_CC_NOMBRE,
trim(descripcion)								DS_CC_NOMBRE,
case when robustecida='1' then 1 else 0 end		FLAG_CC_ROBUSTECIDA,
partition_date									PARTITION_DATE
from bi_corp_staging.rio32_rln_transacciones
where partition_Date= '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_CONTACT_CENTER') }}'

