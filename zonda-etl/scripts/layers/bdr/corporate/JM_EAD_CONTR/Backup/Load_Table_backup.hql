set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=4000000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.exec.dynamic.partition.mode=nonstrict;
set mapred.job.queue.name=root.dataeng;

INSERT OVERWRITE TABLE bi_corp_bdr.JM_EAD_CONTR_bkup partition(partition_date)
SELECT
g5519_feoperac,
g5519_s1emp,
g5519_contra1,
g5519_metaplic,
g5519_fasecalc,
g5519_mtm,
g5519_ead,
g5519_espeprov,
g5519_fecultmo,
g5519_impnomct,
g5519_addonbru,
g5519_addonnet,
g5519_coefregu,
g5519_metliqui,
g5519_mtm_brut,
partition_date
FROM bi_corp_bdr.JM_EAD_CONTR
WHERE partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_Tables-Backup') }}'
;