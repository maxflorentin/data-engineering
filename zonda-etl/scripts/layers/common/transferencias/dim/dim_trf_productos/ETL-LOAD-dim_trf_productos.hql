set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=4000000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.exec.dynamic.partition.mode=nonstrict;
set mapred.job.queue.name=root.dataeng;

--inserto dimension a foto completa en cada particion desde ATRC

INSERT OVERWRITE TABLE bi_corp_common.dim_trf_productos
PARTITION (partition_date)
select distinct
    trim(codigo)                  cod_trf_producto,
    upper(trim(descripcion))      ds_trf_producto,
    case when trim(fecha_ult_modif) in ('9999-12-31', '0001-01-01','0000-00-00')  then null else trim(fecha_ult_modif) end dt_trf_fecha_ult_modif,
    'ATRC'                        cod_trf_origen,
    '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Transferencias') }}' partition_date
from bi_corp_staging.atrc_productos
where partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Transferencias') }}';
