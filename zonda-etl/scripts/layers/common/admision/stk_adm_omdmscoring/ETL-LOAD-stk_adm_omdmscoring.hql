set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=4000000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.exec.dynamic.partition.mode=nonstrict;
set mapred.job.queue.name=root.dataeng;

INSERT OVERWRITE TABLE bi_corp_common.stk_adm_omdmscoring
    PARTITION (partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Admision') }}')
select cod_inqy as cod_adm_tramite,
cast(nro_sec_scoring as int) as cod_adm_nrosecscoring,
nro_scorecard as ds_adm_nroscorecard,
cast(cast(cod_final_score as decimal(15,1)) as bigint) as fc_adm_finalscore,
cod_score_recomend as ds_adm_scorerecomend,
cast(val_corte_final as bigint) as fc_cortefinal,
cast(val_corte_max as bigint) as fc_cortemax,
substr(fec_proceso,1,19) as ts_adm_fecproceso
from bi_corp_staging.scoring_omdm_scoring
where partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Admision') }}';