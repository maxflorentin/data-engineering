set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=4000000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.exec.dynamic.partition.mode=nonstrict;
SET mapred.job.queue.name=root.dataeng;

INSERT OVERWRITE TABLE bi_corp_common.dim_adm_empresas_relacion_lineas_productos
    PARTITION (partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Admision_Empresas-Daily') }}')
select
    substr(cod_producto, 1, 2) as cod_prod_producto,
    substr(cod_producto, 3, 4) as cod_prod_subproducto,
    cod_operacion as cod_adm_operacion,
    peusualt as cod_adm_peusualt,
    pefecalt as ts_adm_pefecalt,
    peusumod as cod_adm_peusumod,
    pefecmod as ts_adm_pefecmod
from bi_corp_staging.sge_lineas_productos
WHERE partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Admision_Empresas-Daily') }}';