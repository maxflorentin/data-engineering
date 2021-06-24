set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=4000000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.exec.dynamic.partition.mode=nonstrict;
set mapred.job.queue.name=root.dataeng;

INSERT OVERWRITE TABLE bi_corp_bdr.jm_ctos_cance_bkup partition(partition_date)
SELECT
h0711_feoperac,
h0711_s1emp,
h0711_contra1,
h0711_motvcanc,
h0711_fecultmo,
partition_date
FROM bi_corp_bdr.jm_ctos_cance
WHERE partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_Tables-Backup') }}'
;