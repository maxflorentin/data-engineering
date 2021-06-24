set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=4000000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.exec.dynamic.partition.mode=nonstrict;
set mapred.job.queue.name=root.dataeng;

INSERT OVERWRITE TABLE bi_corp_bdr.JM_CAL_EXT_CL_bkup partition(partition_date)
SELECT
r9415_feoperac,
r9415_s1emp,
r9415_idnumcli,
r9415_cod_agen,
r9415_ccodplz,
r9415_tipmoned,
r9415_feccalif,
r9415_califmae,
partition_date
FROM bi_corp_bdr.JM_CAL_EXT_CL
WHERE partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_Tables-Backup') }}'
;