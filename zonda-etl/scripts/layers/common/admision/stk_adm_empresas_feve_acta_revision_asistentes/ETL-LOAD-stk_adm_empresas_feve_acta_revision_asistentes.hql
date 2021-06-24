set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=4000000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.exec.dynamic.partition.mode=nonstrict;
SET mapred.job.queue.name=root.dataeng;

INSERT OVERWRITE TABLE bi_corp_common.stk_adm_empresas_feve_acta_revision_asistentes
    PARTITION (partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Admision_Empresas-Daily') }}')
select
    cast(id_acta as int) as id_adm_acta,
    nombrecompleto as ds_adm_nombrecompleto,
    cast(dni as bigint) as cod_adm_dni,
    codcargo as cod_adm_cargo,
    cast(nup as int) as cod_per_nup
from bi_corp_staging.sge_feve_acta_revision_asistentes
WHERE partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Admision_Empresas-Daily') }}';