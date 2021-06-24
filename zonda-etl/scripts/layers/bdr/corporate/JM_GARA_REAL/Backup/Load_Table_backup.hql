set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=4000000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.exec.dynamic.partition.mode=nonstrict;
set mapred.job.queue.name=root.dataeng;

INSERT OVERWRITE TABLE bi_corp_bdr.JM_GARA_REAL_bkup partition(partition_date)
SELECT
g4091_feoperac,
g4091_s1emp,
g4091_biengar1,
g4091_contra1,
g4091_id_prod,
g4091_cod_bien,
g4091_bco_cust,
g4091_bsindi1,
g4091_indcotiz,
g4091_deudapub,
g4091_inddeuda,
g4091_tipfrecu,
g4091_frecplaz,
g4091_dkafecto,
g4091_cla_bien,
g4091_id_pais,
g4091_ord_hipo,
g4091_codivisa,
g4091_fchavcto,
g4091_inf_regi,
g4091_idemis,
g4091_codidper,
g4091_idnumcli,
g4091_emis_pzo,
g4091_codmercd,
g4091_num_titu,
g4091_indicea1,
g4091_tip_inte,
g4091_codcompa,
g4091_gradocob,
g4091_num_doc,
g4091_indacept,
g4091_indddomi,
g4091_indblo,
g4091_imcarg,
g4091_fecultmo,
g4091_codposgr,
g4091_inayudpb,
g4091_tip_hipo,
partition_date
FROM bi_corp_bdr.JM_GARA_REAL
WHERE partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_Tables-Backup') }}'
;