set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=4000000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.exec.dynamic.partition.mode=nonstrict;
set mapred.job.queue.name=root.dataeng;

INSERT OVERWRITE TABLE bi_corp_bdr.jm_flujos_bkup partition(partition_date)
SELECT
r9746_feoperac,
r9746_s1emp,
r9746_contra1,
r9746_fecmvto,
r9746_clasmvto,
r9746_importe,
r9746_salonbal,
partition_date
FROM bi_corp_bdr.jm_flujos
WHERE partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_Tables-Backup') }}'
;