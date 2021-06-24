set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=4000000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.exec.dynamic.partition.mode=nonstrict;
set mapred.job.queue.name=root.dataeng;

INSERT OVERWRITE TABLE bi_corp_bdr.JM_CAL_EMISION_bkup partition(partition_date)
SELECT
e0665_s1emp,
e0665_id_emisi,
e0665_cod_emis,
e0665_cod_agen,
e0665_ccodplz,
e0665_feccali,
e0665_codmercd,
e0665_fechasta,
e0665_califmae,
e0665_fecultmo,
partition_date
FROM bi_corp_bdr.JM_CAL_EMISION
WHERE partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_Tables-Backup') }}'
;