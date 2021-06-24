set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=4000000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.exec.dynamic.partition.mode=nonstrict;
SET mapred.job.queue.name=root.dataeng;

INSERT OVERWRITE TABLE bi_corp_common.dim_adm_ind_productopromocion
PARTITION (partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Admision_Ind-Daily') }}')

select
cast(trim(producto) as int) as cod_adm_producto,
trim(des_producto) as ds_adm_producto,
trim(des_producto_detalle) as ds_adm_productodetalle
from bi_corp_staging.risksql5_admision_productos_promocion
where partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Admision_Ind-Daily') }}'
;