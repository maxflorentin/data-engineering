set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=4000000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.exec.dynamic.partition.mode=nonstrict;
set mapred.job.queue.name=root.dataeng;

INSERT OVERWRITE TABLE bi_corp_bdr.jm_contr_bis_bkup partition(partition_date)
SELECT
g4095_feoperac,
g4095_s1emp,
g4095_contra1,
g4095_idsucur,
g4095_id_pais,
g4095_id_prod,
g4095_idpro_lc,
g4095_fecvento,
g4095_fecve2,
g4095_fechaper,
g4095_feccance,
g4095_fecrees,
g4095_fecrefi,
g4095_fecnovac,
g4095_idfinali,
g4095_idfinald,
g4095_contra2,
g4095_coddiv,
g4095_id_canal,
g4095_id_cana2,
g4095_id_natur,
g4095_id_nsuby,
g4095_indlim,
g4095_indava,
g4095_ind_titu,
g4095_inddeud,
g4095_inddeud2,
g4095_tip_inte,
g4095_idemis,
g4095_idemisi,
g4095_idneting,
g4095_idcolate,
g4095_fecultmo,
g4095_vencorig,
g4095_deudprel,
g4095_fecentvi,
g4095_idprolc2,
partition_date
FROM bi_corp_bdr.jm_contr_bis
WHERE partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_Tables-Backup') }}'
;