set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=4000000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.exec.dynamic.partition.mode=nonstrict;
SET mapred.job.queue.name=root.dataeng;

INSERT OVERWRITE TABLE bi_corp_common.stk_adm_empresas_ventas_balance_propuesta
    PARTITION (partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Admision_Empresas-Daily') }}')
select
    cast(id_venta as bigint) as id_adm_venta,
    cast(id_balance as bigint) as id_adm_balance,
    cast(nro_prop as bigint) as cod_adm_nro_prop,
    cast(mon_venta as double) as fc_adm_mon_venta,
    fec_venta as dt_adm_fec_venta
from bi_corp_staging.sge_vtas_blc_prop
where partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Admision_Empresas-Daily') }}';