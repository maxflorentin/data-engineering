set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=4000000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.exec.dynamic.partition.mode=nonstrict;
set mapred.job.queue.name=root.dataeng;

INSERT OVERWRITE TABLE bi_corp_common.stk_adm_omdmreglas
    PARTITION (partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Admision') }}')
select cod_inqy as cod_adm_tramite,
tpo_registro as ds_adm_tporegistro,
id_conjregla_decision as cod_adm_conjregladecision,
cod_razon as cod_adm_razon,
cast(secuencia as int) as cod_adm_secuencia,
substr(fec_proceso,1,19) as ts_adm_fecproceso
from bi_corp_staging.scoring_omdm_regla_decision_razon
where partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Admision') }}';