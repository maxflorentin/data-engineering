set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=4000000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.exec.dynamic.partition.mode=nonstrict;
set mapred.job.queue.name=root.dataeng;

INSERT OVERWRITE TABLE bi_corp_bdr.JM_INF_AD_CLI_bkup partition(partition_date)
SELECT
h0780_feoperac,
h0780_s1emp,
h0780_idnumcli,
h0780_tipinfrl,
h0780_tipinfrg,
h0780_importh,
h0780_coddiv,
h0780_fechaact,
h0780_fecultmo,
h0780_cuotpres,
h0780_ingclien,
h0780_num_mtos,
h0780_fecingre,
h0780_rdeuding,
h0780_tip_empr,
h0780_tot_ingr,
h0780_tot_deuf,
h0780_flgacsld,
partition_date
FROM bi_corp_bdr.JM_INF_AD_CLI
WHERE partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_Tables-Backup') }}'
;