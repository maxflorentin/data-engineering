set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=4000000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.exec.dynamic.partition.mode=nonstrict;
set mapred.job.queue.name=root.dataeng;

INSERT OVERWRITE TABLE bi_corp_bdr.jm_posic_contr_bkup partition(partition_date)
SELECT
e0621_feoperac,
e0621_s1emp,
e0621_contra1,
e0621_cta_cont,
e0621_tip_impt,
e0621_fec_mes,
e0621_agrctacb,
e0621_importh,
e0621_coddiv,
e0621_fecultmo,
e0621_centctbl,
e0621_ctacgbal,
partition_date
FROM bi_corp_bdr.jm_posic_contr
WHERE partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_Tables-Backup') }}'
;