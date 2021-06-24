set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=4000000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.exec.dynamic.partition.mode=nonstrict;
set mapred.job.queue.name=root.dataeng;

INSERT OVERWRITE TABLE bi_corp_common.stk_adm_empresas_clasificacion_deudores_cliente
    PARTITION (partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Admision_Empresas-Daily') }}')
select
    cast(id_cladeu as bigint) as id_adm_cladeu,
    cast(penumper as bigint) as cod_adm_penumper,
    cast(nro_cta_cte as bigint) as cod_adm_nro_cta_cte,
    clasif_vigente as cod_adm_clasif_vigente,
    clasif_definitiva as cod_adm_clasif_definitiva,
    com_plan_accion as ds_adm_com_plan_accion,
    com_comite_banca as ds_adm_com_comite_banca
from bi_corp_staging.sge_clasificacion_deudores_per
where partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Admision_Empresas-Daily') }}';