set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=4000000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.exec.dynamic.partition.mode=nonstrict;
set mapred.job.queue.name=root.dataeng;

INSERT OVERWRITE TABLE bi_corp_bdr.jm_clien_juri_bkup partition(partition_date)
SELECT
g5508_feoperac,
g5508_s1emp,
g5508_idnumcli,
g5508_inffecha,
g5508_impfactm,
g5508_tot_acti,
g5508_num_empl,
g5508_orig_fac,
g5508_orig_act,
g5508_orig_emp,
g5508_fecultmo,
g5508_tdeudacl,
g5508_rat_cet1,
g5508_tasamora,
g5508_tot_eqty,
g5508_orgdepen,
g5508_flgempno,
partition_date
FROM bi_corp_bdr.jm_clien_juri
WHERE partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_Tables-Backup') }}'
;