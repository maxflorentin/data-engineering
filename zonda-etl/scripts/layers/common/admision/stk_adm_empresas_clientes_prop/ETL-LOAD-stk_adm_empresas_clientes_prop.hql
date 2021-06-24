set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=4000000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.exec.dynamic.partition.mode=nonstrict;
set mapred.job.queue.name=root.dataeng;

INSERT OVERWRITE TABLE bi_corp_common.stk_adm_empresas_clientes_prop
    PARTITION (partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Admision_Empresas-Daily') }}')
SELECT
cast(c.nro_prop as int) as cod_adm_nro_prop,
nom_cliente as ds_adm_nom_cliente,
cast(c.cod_cliente as int) as cod_adm_cliente,
cast(c.plz_cobro as int) as int_adm_plz_cobro,
cast(c.porc_ventas as int) as int_adm_porc_ventas,
c.ref_obtenidas as ds_adm_ref_obtenidas
from bi_corp_staging.sge_clientes_prop c
where partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Admision_Empresas-Daily') }}';
