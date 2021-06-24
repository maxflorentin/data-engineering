set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=4000000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.exec.dynamic.partition.mode=nonstrict;
set mapred.job.queue.name=root.dataeng;

insert OVERWRITE table bi_corp_bdr.test_jm_contr_otros
partition(partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_Tactico_Mitre') }}')
--Todo menos Tactico Mitre
select co.e0623_feoperac, co.e0623_s1emp,    co.e0623_contra1,  co.e0623_fec_mes,  co.e0623_vcto_res, 
       co.e0623_vcto_pon, co.e0623_idsubprd, co.e0623_tip_liqu, co.e0623_liq_pzo,  co.e0623_tip_amor,
       co.e0623_amor_pzo, co.e0623_amor_sis, co.e0623_ctb_situ, co.e0623_gest_sit, co.e0623_ges2_sit,
       co.e0623_ataemax,  co.e0623_tip_int,  co.e0623_imp1limi, co.e0623_alimact,  co.e0623_importh,
       co.e0623_inv_nego, co.e0623_iprovisi, co.e0623_iprovis1, co.e0623_fecultmo, co.e0623_estadtrj,
       co.e0623_inactrj,  co.e0623_unnt,     co.e0623_unnts,    co.e0623_unnv,     co.e0623_unnvs,
       co.e0623_rgosub,   co.e0623_fecincar, co.e0623_fecficar, co.e0623_intneg,   co.e0623_mtvalta,
       co.e0623_indsubro, co.e0623_tip_inte, co.e0623_difernci, co.e0623_imprtcuo, co.e0623_indsegur,
       co.e0623_amortpar, co.e0623_fecimpag, co.e0623_impprimp, co.e0623_fhprimpg, co.e0623_impimpnr,
       co.e0623_estprinm, co.e0623_exclcto,  co.e0623_cuotimpa, co.e0623_limocult, co.e0623_codimphi,
       co.e0623_indeucon, co.e0623_tipcaren, co.e0623_cuotpres, co.e0623_ibuysell, co.e0623_sutipint,
       co.e0623_tetipint, co.e0623_tipsuelo, co.e0623_tiptecho, co.e0623_plrevtin, co.e0623_feccuota,
       co.e0623_nudiaatr, co.e0623_segpllim, co.e0623_voltrans, co.e0623_marcauti, co.e0623_topdeale,
       co.e0623_fecrepre, co.e0623_emprepor, co.e0623_impcuimp, co.e0623_tipinefe, co.e0623_forpgact,
       co.e0623_finiutct, co.e0623_ffinutct 
from bi_corp_bdr.jm_contr_otros co
   left join bi_corp_bdr.test_normalizacion_id_contratos_history nch
      on co.e0623_contra1 = nch.id_cto_bdr
      and nch.source != 'Tactico-Mitre' 
where co.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_Tactico_Mitre') }}'
union all
--Tactico Mitre
select '{{ ti.xcom_pull(task_ids='InputConfig', key='last_calendar_day', dag_id='BDR_LOAD_Tactico_Mitre') }}' AS e0623_feoperac ,
       tm.empresa_CO    AS e0623_s1emp,
       m.id_cto_bdr     AS e0623_contra1,
       '{{ ti.xcom_pull(task_ids='InputConfig', key='last_calendar_day', dag_id='BDR_LOAD_Tactico_Mitre') }}' AS e0623_fec_mes,
       concat(lpad(datediff(cb.g4095_fecvento, '{{ ti.xcom_pull(task_ids='InputConfig', key='last_calendar_day', dag_id='BDR_LOAD_Tactico_Mitre') }}'),11,'0'),'000000') AS e0623_vcto_res,
       lpad(0, 17, '0') AS e0623_vcto_pon,
       tm.idsubprd_CO   AS e0623_idsubprd,
       lpad(0, 5, '0')  AS e0623_tip_liqu,
       lpad(0, 17, '0') AS e0623_liq_pzo,
       lpad(0, 5, '0')  AS e0623_tip_amor,
       lpad(0, 17, '0') AS e0623_amor_pzo,
       lpad(0, 5, '0')  AS e0623_amor_sis,
       lpad(0, 5, '0')  AS e0623_ctb_situ,
       tm.gest_sit_CO   AS e0623_gest_sit,
       tm.ges2_sit_CO   AS e0623_ges2_sit,
       lpad(0, 9, '0')  AS e0623_ataemax,
       lpad(0, 9, '0')  AS e0623_tip_int,
       concat('+', lpad(0, 16, '0')) AS e0623_imp1limi,
       concat('+', lpad(0, 16, '0')) AS e0623_alimact,
       concat('+', lpad(0, 16, '0')) AS e0623_importh,
       lpad(0, 5, '0')  AS e0623_inv_nego,
       concat('+', lpad(0, 16, '0')) AS e0623_iprovisi,
       concat('+', lpad(0, 16, '0')) AS e0623_iprovis1,
       '{{ ti.xcom_pull(task_ids='InputConfig', key='last_calendar_day', dag_id='BDR_LOAD_Tactico_Mitre') }}' AS e0623_fecultmo,
       lpad(0, 5, '0')  AS e0623_estadtrj,
       lpad(0, 17, '0') AS e0623_inactrj,
       lpad(0, 5, '0')  AS e0623_unnt,
       lpad(0, 5, '0')  AS e0623_unnts,
       lpad(0, 5, '0')  AS e0623_unnv,
       lpad(0, 5, '0')  AS e0623_unnvs,
       ' '              AS e0623_rgosub,
       '0001-01-01'     AS e0623_fecincar,
       '0001-01-01'     AS e0623_fecficar,
       ' '              AS e0623_intneg,
       lpad(0, 5, '0')  AS e0623_mtvalta,
       ' '              AS e0623_indsubro,
       lpad(0, 5, '0')  AS e0623_tip_inte,
       lpad(0, 9, '0')  AS e0623_difernci,
       concat('+', lpad(0, 16, '0')) AS e0623_imprtcuo,
       ' '              AS e0623_indsegur,
       ' '              AS e0623_amortpar,
       '9999-12-31'     AS e0623_fecimpag,
       concat('-', lpad(0, 16, '0')) AS e0623_impprimp,
       '9999-12-31'     AS e0623_fhprimpg ,
       concat('-', lpad(0, 16, '0')) AS e0623_impimpnr,
       lpad(0, 5, '0')  AS e0623_estprinm,
       lpad(0, 5, '0')  AS e0623_exclcto,
       lpad(0, 9, '0')  AS e0623_cuotimpa,
       concat('+', lpad(0, 16, '0')) AS e0623_limocult,
       lpad(0, 5, '0')  AS e0623_codimphi,
       ' '              AS e0623_indeucon,
       lpad(0, 5, '0')  AS e0623_tipcaren,
       concat('+', lpad(0, 16, '0')) AS e0623_cuotpres,
       ' '              AS e0623_ibuysell,
       lpad(0, 9, '0')  AS e0623_sutipint,
       lpad(0, 9, '0')  AS e0623_tetipint,
       lpad(0, 5, '0')  AS e0623_tipsuelo,
       lpad(0, 5, '0')  AS e0623_tiptecho,
       lpad(0, 5, '0')  AS e0623_plrevtin,
       '9999-12-31'     AS e0623_feccuota,
       lpad(0, 9, '0')  AS e0623_nudiaatr,
       lpad(0, 5, '0')  AS e0623_segpllim,
       concat('+', lpad(0, 16, '0')) AS e0623_voltrans,
       ' '              AS e0623_marcauti,
       lpad(0, 5, '0')  AS e0623_topdeale,
       '9999-12-31'     AS e0623_fecrepre,
       tm.emprepor_CO   AS e0623_emprepor,
       concat('+', lpad(0, 16, '0')) AS e0623_impcuimp,
       lpad(0, 9, '0')  AS e0623_tipinefe,
       lpad(0, 5, '0')  AS e0623_forpgact,
       tm.finiutct_CO   AS e0623_finiutct,
       tm.ffinutct_CO   AS e0623_ffinutct 
FROM bi_corp_bdr.tactico_mitre_contratos tm
   INNER JOIN bi_corp_bdr.test_normalizacion_id_contratos m
      ON tm.contrato = m.id_cto_source
      AND m.source = 'Tactico-Mitre'
      AND m.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_Tactico_Mitre') }}'
   INNER JOIN bi_corp_bdr.test_jm_contr_bis cb
      ON m.id_cto_bdr = cb.g4095_contra1
      AND cb.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_Tactico_Mitre') }}'
WHERE '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_Tactico_Mitre') }}' between tm.fecha_desde and tm.fecha_hasta;