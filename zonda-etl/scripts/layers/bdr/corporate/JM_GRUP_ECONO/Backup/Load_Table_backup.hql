set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=4000000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.exec.dynamic.partition.mode=nonstrict;
set mapred.job.queue.name=root.dataeng;

INSERT OVERWRITE TABLE bi_corp_bdr.jm_grup_econo_bkup partition(partition_date)
SELECT
g5512_feoperac,
g5512_s1emp,
g5512_grup_eco,
g5512_dcodgrup,
g5512_dgrupo,
g5512_idsucur,
g5512_id_pais,
g5512_impfactm,
g5512_tot_acti,
g5512_num_empl,
g5512_orig_fac,
g5512_orig_act,
g5512_orig_emp,
g5512_impt_rgo,
g5512_fecinfac,
g5512_grecosec,
g5512_coddiv,
g5512_fecultmo,
g5512_tdeudagr,
g5512_flgempno,
partition_date
FROM bi_corp_bdr.jm_grup_econo
WHERE partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_Tables-Backup') }}'
;