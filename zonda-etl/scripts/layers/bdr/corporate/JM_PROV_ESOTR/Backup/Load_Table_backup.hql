set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=4000000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.exec.dynamic.partition.mode=nonstrict;
set mapred.job.queue.name=root.dataeng;

INSERT OVERWRITE TABLE bi_corp_bdr.JM_PROV_ESOTR_bkup partition(partition_date)
SELECT
n0625_feoperac,
n0625_s1emp,
n0625_contra1,
n0625_tip_impt,
n0625_importh,
n0625_coddiv,
n0625_cta_cont,
n0625_agrctacb,
n0625_centctbl,
n0625_ctacgbal,
n0625_stage,
partition_date
FROM bi_corp_bdr.JM_PROV_ESOTR
;