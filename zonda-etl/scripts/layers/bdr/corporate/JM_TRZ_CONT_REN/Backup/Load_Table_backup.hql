set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=4000000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.exec.dynamic.partition.mode=nonstrict;
set mapred.job.queue.name=root.dataeng;

INSERT OVERWRITE TABLE bi_corp_bdr.jm_trz_cont_ren_bkup partition(partition_date)
SELECT
g7025_s1emp,
g7025_contra1,
g7025_emp_ant,
g7025_cont_ant,
g7025_motrenu,
g7025_fealtrel,
g7025_fec_mod,
g7025_imprestr,
g7025_coddiv,
g7025_motrenug,
g7025_fec_baja,
partition_date
FROM bi_corp_bdr.jm_trz_cont_ren
WHERE partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_Tables-Backup') }}'
;