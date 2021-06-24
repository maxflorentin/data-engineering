set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=4000000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.exec.dynamic.partition.mode=nonstrict;
set mapred.job.queue.name=root.dataeng;

INSERT OVERWRITE TABLE bi_corp_bdr.JM_VAL_GARA_bkup partition(partition_date)
SELECT
g4134_feoperac,
g4134_s1emp,
g4134_biengar1,
g4134_fechvalr,
g4134_nomenti,
g4134_imgarant,
g4134_fecultmo,
g4134_tipvaln,
g4134_tip_gara,
g4134_coddiv,
partition_date
FROM bi_corp_bdr.JM_VAL_GARA
WHERE partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_Tables-Backup') }}'
;