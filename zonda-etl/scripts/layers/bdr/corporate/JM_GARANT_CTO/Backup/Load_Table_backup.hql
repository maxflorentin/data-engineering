set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=4000000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.exec.dynamic.partition.mode=nonstrict;
set mapred.job.queue.name=root.dataeng;

INSERT OVERWRITE TABLE bi_corp_bdr.JM_GARANT_CTO_bkup partition(partition_date)
SELECT
g4124_feoperac,
g4124_s1emp,
g4124_contra1,
g4124_tip_gara,
g4124_biengar1,
g4124_fecini,
g4124_fecbaja,
g4124_fecvcto,
g4124_cod_gar,
g4124_cod_gar2,
g4124_tipo_ins,
g4124_ind_pign,
g4124_tip_aval,
g4124_tip_cobe,
g4124_est_gara,
g4124_porcober,
g4124_cob_inic,
g4124_impt_nom,
g4124_imp_actu,
g4124_comf_let,
g4124_fecultmo,
g4124_num_aseg,
g4124_coddiv,
g4124_idnumcli,
g4124_indblo,
g4124_indrgosb,
g4124_indcobpf,
g4124_valaseju,
g4124_repaporc,
g4124_ordapgar,
g4124_rangohip,
g4124_n_impago,
partition_date
FROM bi_corp_bdr.JM_GARANT_CTO
WHERE partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_Tables-Backup') }}'
;