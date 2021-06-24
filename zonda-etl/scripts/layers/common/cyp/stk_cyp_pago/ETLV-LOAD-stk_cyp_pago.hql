set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
SET mapred.job.queue.name=root.dataeng;


INSERT OVERWRITE TABLE  bi_corp_common.stk_cyp_pago
PARTITION(partition_date)

select
warce003_ide_pgo								cod_cyp_ide_pgo,
warce003_nro_empr								cod_cyp_nro_empr,
warce003_nro_dig_empr							cod_cyp_nro_digempr,
warce003_nro_prod								cod_cyp_nro_prod,
warce003_nro_instan								ds_cyp_nro_instan,
warce003_nro_clte								cod_cyp_nro_clte,
warce003_cod_lst_pre							cod_cyp_lst_pre,
warce003_nro_suc								cod_cyp_suc,
warce003_nro_bco_corrs							ds_cyp_nro_bcocorrs,
warce003_fec_pgo								dt_cyp_fec_pag,
TRANSLATE(warce003_hra_pgo,'/',':')				ds_cyp_hra_pgo,
warce003_cod_fp_com_3ra							cod_cyp_fp_com,
warce003_tpo_cta_com_3ra						cod_cyp_tpo_ctacom,
warce003_nro_succta_com_3ra						cod_cyp_suc_ctacom,
warce003_nro_cta_com_3ra						cod_cyp_cta_com,
warce003_nro_dv_cta_com_3ra						ds_cyp_digito_ctacom,
warce003_imp_pgo_com_3ra						fc_cyp_imp_pgocom,
warce003_can_doc								ds_cyp_can_doc,
warce003_tot_pgo								fc_cyp_tot_pgo,
warce003_tot_int_puni							fc_cyp_tot_intpuni,
warce003_tot_bonf								fc_cyp_tot_bonf,
warce003_imp_cotiz_dl_vend						fc_cyp_imp_cotizvend,
warce003_imp_cotiz_dl_comp						fc_cyp_imp_cotizcomp,
warce003_cod_trn								ds_cyp_trn,
warce003_imp_com_3ra							fc_cyp_imp_com3ra,
warce003_imp_com_3ra_discr						fc_cyp_imp_com3radiscr,
warce003_imp_iva_3ra							fc_cyp_imp_iva3ra,
warce003_imp_iva_adic_3ra						fc_cyp_imp_ivaadic3ra,
warce003_imp_ing_bru_3ra						fc_cyp_imp_ingbru3ra,
warce003_nom_clte								ds_cyp_nom_clte,
warce003_nro_cuit_clte							ds_cyp_nro_cuitclte,
CASE WHEN trim(warce003_cod_ing_bru)=''
	 THEN NULL ELSE warce003_cod_ing_bru END 	cod_cyp_ing_bru,
CASE WHEN trim(warce003_cod_cond_iva)=''
	 THEN NULL ELSE warce003_cod_cond_iva END 	cod_cyp_condiva,
warce003_nro_cob_infinity						ds_cyp_nro_cobinfinity,
warce003_imp_com_empr							fc_cyp_com_empre,
warce003_imp_com_empr_discr						fc_cyp_com_emprediscr,
warce003_imp_iva_empr							fc_cyp_iva_empr,
warce003_imp_iva_adic_empr						fc_cyp_iva_adicempr,
warce003_imp_ing_bru_empr						fc_cyp_imp_ingbruempr,
warce003_nro_ult_rend							ds_cyp_nro_ultrend,
CASE WHEN TRIM(warce003_fec_ult_rend)='0001-01-01'
	 THEN NULL ELSE TRIM(warce003_fec_ult_rend) END dt_cyp_fec_ultrend,
warce003_cod_est_rendicion						cod_cyp_est_rend,
warce003_can_doc_pdtes_acr						ds_cyp_can_docpend,
warce003_cod_est_pgo							cod_cyp_est_pgo,
warce003_fec_est_pgo							dt_cyp_fec_estpgo,
warce003_mar_procesam							ds_cyp_mar_procesam,
CASE WHEN TRIM(warce003_fec_alta_envio)='0001-01-01'
	 THEN NULL ELSE TRIM(warce003_fec_alta_envio) END 							dt_cyp_fec_altaenvio,
warce003_nro_envio								ds_cyp_nro_envio,
warce003_nro_orig_inf							ds_cyp_nro_originf,
warce003_can_otr_impre							ds_cyp_cant_impre,
warce003_can_liq								ds_cyp_can_liq,
warce003_can_doc_pdtes_emit						ds_cyp_doc_pdtesemis,
warce003_mar_inh_baja							ds_cyp_mar_inhbaja,
CASE WHEN warce003_fec_baja='0001-01-01'
	 THEN NULL ELSE warce003_fec_baja END		dt_cyp_fec_baja,
CASE WHEN trim(warce003_cod_canal)=''
	 THEN NULL ELSE warce003_cod_canal END 								cod_cyp_canal,
CASE WHEN trim(warce003_cod_sub_canal)=''
	 THEN NULL ELSE warce003_cod_sub_canal END 							cod_cyp_subcanal,
CASE WHEN trim(waprp030_tpo_doc)=''
	 THEN NULL ELSE waprp030_tpo_doc END 			ds_cyp_tpo_doc,
waprp030_nro_doc								ds_cyp_nro_doc,
CASE WHEN trim(waprp030_tpo_doc_1)=''
	 THEN NULL ELSE waprp030_tpo_doc_1 END 								ds_cyp_tpo_doc1,
waprp030_nro_doc_1								ds_cyp_nro_doc1,
CASE WHEN trim(waprp030_tpo_doc_2)=''
	 THEN NULL ELSE waprp030_tpo_doc_2 END 								ds_cyp_tpo_doc2,
waprp030_nro_doc_2								ds_cyp_nro_doc2,
waprp030_tpo_recibo								ds_cyp_tpo_rec,
waprp030_nro_secu_dom							ds_cyp_nro_secu_dom,
waspe006_cod_docum								cod_cyp_docum,
waspe006_nro_docum								cod_cyp_nrodocum,
waspe006_nro_sec_docum							cod_cyp_nro_secdocum,
waspe006_nro_envio								cod_cyp_nro_envio,
waspe006_cod_est								cod_cyp_estado,
waspe006_nro_suc_dist							cod_cyp_suc_dist,
waspe006_tpo_dist								cod_cyp_tpo_dist,
waspe006_nro_empr_dist							cod_cyp_nro_empredist,
waspe006_fec_emision							dt_cyp_fec_emision,
waspe006_fec_emision_calc						dt_cyp_fec_emisioncalc,
waspe006_des_detall_impre_text					ds_cyp_detalle,
war003.partition_date

from  bi_corp_staging.arce_warce003 war003

left join  bi_corp_staging.aprp_waprp030 war030 on (war003.warce003_ide_pgo=war030.waprp030_ide_pgo and war030.partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='working_day', dag_id='LOAD_CMN_COBROSYPAGOS') }}' )
left join bi_corp_staging.aspe_waspe006 was006 on (war003.warce003_ide_pgo=was006.waspe006_ide_pgo and was006.partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='working_day', dag_id='LOAD_CMN_COBROSYPAGOS') }}' )
where war003.partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='working_day', dag_id='LOAD_CMN_COBROSYPAGOS') }}' ;