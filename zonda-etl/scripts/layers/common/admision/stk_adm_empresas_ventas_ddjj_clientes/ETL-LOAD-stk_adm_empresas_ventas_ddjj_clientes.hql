set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=4000000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.exec.dynamic.partition.mode=nonstrict;
set mapred.job.queue.name=root.dataeng;

INSERT OVERWRITE TABLE bi_corp_common.stk_adm_empresas_ventas_ddjj_clientes
    PARTITION (partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Admision_Empresas-Daily') }}')
select
    cast(id_ddjj as bigint) as id_adm_ddjj,
    cast(penumper as bigint) as cod_adm_penumper,
    cast(id_venta as bigint) as id_adm_venta,
    cast(mon_venta as decimal(15,2)) as fc_adm_venta,
    date_format(fec_venta, 'yyyy-MM-dd') as dt_adm_venta
from bi_corp_staging.sge_vtas_ddjj_per
where partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Admision_Empresas-Daily') }}';