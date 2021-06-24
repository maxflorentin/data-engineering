set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=4000000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.exec.dynamic.partition.mode=nonstrict;
set mapred.job.queue.name=root.dataeng;

INSERT OVERWRITE TABLE bi_corp_bdr.jm_expos_no_con_bkup partition(partition_date)
SELECT
e0627_feoperac,
e0627_s1emp,
e0627_contra1,
e0627_fec_mes,
e0627_cta_cont,
e0627_agrctacb,
e0627_importh,
e0627_idctacen,
e0621_fecultmo,
e0627_centctbl,
e0627_entcgbal,
e0627_ctacgbal,
partition_date
FROM bi_corp_bdr.jm_expos_no_con
WHERE partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_Tables-Backup') }}'
;