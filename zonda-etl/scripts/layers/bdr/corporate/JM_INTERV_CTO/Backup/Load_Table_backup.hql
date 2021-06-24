set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=4000000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.exec.dynamic.partition.mode=nonstrict;
set mapred.job.queue.name=root.dataeng;

INSERT OVERWRITE TABLE bi_corp_bdr.jm_interv_cto_bkup partition(partition_date)
SELECT
g4128_feoperac,
g4128_s1emp,
g4128_contra1,
g4128_tipintev,
g4128_tipintv2,
g4128_numordin,
g4128_idnumcli,
g4128_formintv,
g4128_porpartn,
g4128_fecultmo,
partition_date
FROM bi_corp_bdr.jm_interv_cto
WHERE partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_Tables-Backup') }}'
;