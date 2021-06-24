set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=4000000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.exec.dynamic.partition.mode=nonstrict;
set mapred.job.queue.name=root.dataeng;

INSERT OVERWRITE TABLE bi_corp_bdr.jm_grup_rela_bkup partition(partition_date)
SELECT
g5515_feoperac,
g5515_s1emp,
g5515_grup_eco,
g5515_idnumcli,
g5515_rol_jera,
g5515_fecultmo,
partition_date
FROM bi_corp_bdr.jm_grup_rela
WHERE partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_Tables-Backup') }}'
;