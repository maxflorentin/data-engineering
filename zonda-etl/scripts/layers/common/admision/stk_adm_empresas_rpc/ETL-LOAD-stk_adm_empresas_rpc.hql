set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=4000000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.exec.dynamic.partition.mode=nonstrict;
SET mapred.job.queue.name=root.dataeng;

INSERT OVERWRITE TABLE bi_corp_common.stk_adm_empresas_rpc
    PARTITION (partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Admision_Empresas-Daily') }}')
select
    cast(id_rpc as bigint) as id_adm_rpc,
    cast(nro_prop as bigint) as cod_adm_nro_prop,
    cast(imp_importe_calif as decimal(16, 2)) as fc_adm_imp_importe_calif,
    cast(imp_resp_pat_comp as decimal(16, 2)) as fc_adm_imp_resp_pat_comp,
    justificacion as ds_adm_justificacion,
    date_format(fec_calificacion,'yyyy-MM-dd') as dt_adm_calificacion,
    cod_usu_alta as cod_adm_usu_alta,
    date_format(fec_estados_contables,'yyyy-MM-dd') as dt_adm_estados_contables,
    cast(duracion as int) as int_adm_duracion,
    cast(rel_porc as int) as int_adm_rel_porc,
    cast(penumper as int) as cod_adm_penumper,
    mar_imprimir_acta as flag_adm_imprimir_acta
from bi_corp_staging.sge_rpc
where partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Admision_Empresas-Daily') }}';