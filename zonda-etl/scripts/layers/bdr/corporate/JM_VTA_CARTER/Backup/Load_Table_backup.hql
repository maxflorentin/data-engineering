set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=4000000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.exec.dynamic.partition.mode=nonstrict;
set mapred.job.queue.name=root.dataeng;

INSERT OVERWRITE TABLE bi_corp_bdr.jm_vta_carter_bkup partition(partition_date)
SELECT
r9736_feoperac,
r9736_s1emp,
r9736_contra1,
r9736_fvtacart,
r9736_imppdte,
r9736_precioob,
r9736_ind_credit,
partition_date
FROM bi_corp_bdr.jm_vta_carter
WHERE partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_Tables-Backup') }}'
;