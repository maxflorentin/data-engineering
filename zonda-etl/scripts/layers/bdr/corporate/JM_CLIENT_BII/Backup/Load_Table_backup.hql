set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=4000000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.exec.dynamic.partition.mode=nonstrict;
set mapred.job.queue.name=root.dataeng;

INSERT OVERWRITE TABLE bi_corp_bdr.jm_client_bii_bkup partition(partition_date)
SELECT
g4093_feoperac,
g4093_s1emp,
g4093_idnumcli,
g4093_tip_pers,
g4093_apnomper,
g4093_datexto1,
g4093_datexto2,
g4093_identper,
g4093_codidper,
g4093_clie_glo,
g4093_idsucur,
g4093_carter,
g4093_id_pais,
g4093_cod_sect,
g4093_cod_sec2,
g4093_cod_sec3,
g4093_clisegm,
g4093_clisegl1,
g4093_tipsegl1,
g4093_clisegl2,
g4093_tipsegl2,
g4093_fchini,
g4093_fchfin,
g4093_fecultmo,
g4093_industry,
g4093_exclcli,
g4093_indbcart,
g4093_fecnacim,
g4093_paisneg,
g4093_cdpostal,
g4093_tto_espe,
g4093_gra_vinc,
g4093_utp_cli,
g4093_finiutcl,
g4093_ffinutcl,
partition_date
FROM bi_corp_bdr.jm_client_bii
WHERE partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_Tables-Backup') }}'
;