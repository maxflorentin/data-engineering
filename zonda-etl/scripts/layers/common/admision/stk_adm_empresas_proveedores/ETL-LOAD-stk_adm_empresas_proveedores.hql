set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=4000000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.exec.dynamic.partition.mode=nonstrict;
SET mapred.job.queue.name=root.dataeng;

INSERT OVERWRITE TABLE bi_corp_common.stk_adm_empresas_proveedores
    PARTITION (partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Admision_Empresas-Daily') }}')
SELECT
    cast(p.penumper as int) as cod_adm_penumper,
    cast(p.nro_ultima_prop as int) as cod_adm_nro_ultima_prop,
    cast(p.cod_prov as int) as cod_adm_prov,
    p.nom_prov as ds_adm_prov,
    cast(p.plz_pago as int) as int_adm_plz_pago,
    cast(p.porc_compras as int) as int_adm_porc_compras,
    p.ref_obt as cod_adm_ref_obt,
    p.cod_usu_alta as cod_adm_usu_alta,
    p.fec_alta as dt_adm_fec_alta,
    p.cod_usu_mod as cod_adm_usu_mod,
    p.fec_mod as dt_adm_fec_mod
FROM bi_corp_staging.sge_proveedores_per p
WHERE partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Admision_Empresas-Daily') }}';