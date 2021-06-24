set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
SET mapred.job.queue.name=root.dataeng;


INSERT OVERWRITE TABLE bi_corp_common.stk_cyp_instrumentospago
PARTITION(partition_date)

select
warce004_ide_pgo    											cod_cyp_ide_pgo,
warce004_cod_form_pgo   										cod_cyp_form_pgo,
warce004_cod_mone      											cod_cyp_moneda,
warce004_nro_instr      										ds_cyp_nro_instr,
warce004_nro_empr          										cod_cyp_nro_empr,
warce004_nro_dig_empr      										cod_cyp_nro_digempr,
warce004_nro_prod          										cod_cyp_nro_prod,
warce004_nro_instan        										ds_cyp_nro_instan,
warce004_nro_clte          										cod_cyp_nro_clte,
CASE WHEN warce004_fec_acred='0001-01-01'
	 THEN NULL ELSE warce004_fec_acred	END						dt_cyp_fec_acred,
warce004_imp               										fc_cyp_imp,
CASE WHEN warce004_fec_acred_old='0001-01-01'
	 THEN NULL ELSE warce004_fec_acred_old	END					dt_cyp_fec_acdredold,
warce004_imp_old           										fc_cyp_imp_old,
CASE WHEN trim(warce004_mar_acred)=''
	 THEN NULL ELSE trim(warce004_mar_acred) END 		 		ds_cyp_mar_acred,
CASE WHEN trim(warce004_cod_aplic)=''
	 THEN NULL ELSE trim(warce004_cod_aplic) END          		cod_cyp_aplic,
CASE WHEN trim(warce004_cod_mot_rechazo)=''
	 THEN NULL ELSE trim(warce004_cod_mot_rechazo) END    		cod_cyp_mot_rechazo,
warce004_nro_sec_movim     										ds_cyp_nro_secmovim,
CASE WHEN warce004_fec_movim='0001-01-01'
	 THEN NULL ELSE warce004_fec_movim	END         			dt_cyp_fec_movim,
CASE WHEN warce004_fec_vto_cpd='0001-01-01'
	 THEN NULL ELSE warce004_fec_vto_cpd	END         		dt_cyp_fec_vtocpd,
warce004_can_dia_dife      										ds_cyp_cant_diadife,
CASE WHEN trim(warce004_cod_no_a_la_orden)=''
		  THEN NULL ELSE warce004_cod_no_a_la_orden END			cod_cyp_no_alaorden,
CASE WHEN trim(warce004_mar_confirming)=''
		  THEN NULL ELSE warce004_mar_confirming END    		ds_cyp_mar_confirming,
CASE WHEN trim(warce004_mar_float)=''
		  THEN NULL ELSE warce004_mar_float END         		ds_cyp_mar_float,
warce004_cod_est_instr     										cod_cyp_est_instr,
CASE WHEN warce004_fec_emision_calc='0001-01-01'
	 THEN NULL ELSE warce004_fec_emision_calc	END   			dt_cyp_fec_emisioncalc,
CASE WHEN trim(warce004_mar_soli_impre)='S'
		  THEN 1 ELSE 0 END    									flag_cyp_soli_impre,
CASE WHEN warce004_fec_emision='0001-01-01'
	 THEN NULL ELSE warce004_fec_emision	END        			dt_cyp_fec_emision,
CASE WHEN trim(warce004_mar_cuadre)=''
	 THEN NULL ELSE trim(warce004_mar_cuadre) END 				ds_cyp_mar_cuadre,
CASE WHEN warce004_fec_cuadre='0001-01-01'
	 THEN NULL ELSE warce004_fec_cuadre	END         			dt_cyp_fec_cuadre,
warce004_nro_emision       										ds_cyp_nro_emision,
CAST(warce004_nro_suc_dist AS INT)     							cod_cyp_suc_dist,
warce004_tpo_dist          										ds_cyp_tpo_dist,
warce004_nro_empr_dist     										cod_cyp_nro_emprdist,
CASE WHEN warce004_fec_alta_envio='0001-01-01'
	 THEN NULL ELSE warce004_fec_alta_envio	END     			dt_cyp_fec_altaenvio,
warce004_nro_envio         										ds_cyp_nroenvio,
CASE WHEN warce004_fec_tope_procesam='0001-01-01'
	 THEN NULL ELSE warce004_fec_tope_procesam	END  			dt_cyp_fec_topeprocesam,
warce004_nro_cuit_clte     										ds_cyp_nro_cuitclte,
warce004_nom_clte          										ds_cyp_nom_clte,
CASE WHEN warce004_fec_deb='0001-01-01'
	 THEN NULL ELSE warce004_fec_deb	END            			dt_cyp_fec_deb,
warce004_cod_est_deb       										cod_cyp_est_deb,
warce004_mar_soli_rescate  										ds_cyp_mar_solirescate,
CASE WHEN warce004_fec_gest_resc_baj='0001-01-01'
	 THEN NULL ELSE warce004_fec_gest_resc_baj	END  			dt_cyp_fec_gestrescbaj,
CASE WHEN trim(warce004_ide_cta_deb_cred)=''
	 THEN NULL ELSE trim(warce004_ide_cta_deb_cred) END   		cod_cyp_cuenta_debcred,
warce004_imp_cotiz_dl_vend 										fc_cyp_cotiz_vend,
warce004_imp_cotiz_dl_comp 										fc_cyp_cotiz_comp,
CASE WHEN TRIM(warce004_mar_negociado)='S'
	 THEN 1 ELSE 0 END											flag_cyp_negociado,
CASE WHEN warce004_fec_vto_emision='0001-01-01'
	 THEN NULL ELSE warce004_fec_vto_emision	END    			dt_cyp_fec_vtoemision,
CASE WHEN trim(warce004_nom_empr_cheq)=''
	 THEN NULL ELSE trim(warce004_nom_empr_cheq) END      		ds_cyp_nomb_emprecheq,
CASE WHEN trim(warce004_mar_inh_soli_impre)='S'
	 THEN 1 ELSE 0 END 											flag_cyp_inh_soliimpre,
CASE WHEN trim(warce004_mar_interface)=''
	 THEN NULL ELSE trim(warce004_mar_interface) END      		ds_cyp_mar_interface,
CASE WHEN warce004_fec_interface='0001-01-01'
	 THEN NULL ELSE warce004_fec_interface	END      		    dt_cyp_fec_interface,
warce004_cod_est_rend      										cod_cyp_est_rend,
warce004_nro_suc_orig_movim             						cod_cyp_suc_origmovim,
CASE WHEN trim(warce004_mar_acred_inte)=''
	 THEN NULL ELSE trim(warce004_mar_acred_inte) END    		ds_cyp_mar_acredinte,
warce004_nom_benf_cheq     										ds_cyp_nom_benfcheq,
CASE WHEN warce004_fec_est_instr='0001-01-01'
	 THEN NULL ELSE warce004_fec_est_instr	END      			dt_cyp_fec_estinstr,
CASE WHEN trim(warce004_cod_mot_rend)=''
	 THEN NULL ELSE trim(warce004_cod_mot_rend) END 			cod_cyp_mot_rend,
waprp029_tpo_pieza												ds_cyp_tpo_pieza,
CASE WHEN trim(waprp029_nom_archivo)=''
	 THEN NULL ELSE trim(waprp029_nom_archivo) END 				ds_cyp_nom_archivo,
CASE WHEN trim(waprp029_tpo_destino)=''
	 THEN NULL ELSE trim(waprp029_tpo_destino) END 				ds_cyp_tpo_destino,
CASE WHEN trim(waprp029_nom_destino)=''
	 THEN NULL ELSE trim(waprp029_nom_destino) END 				ds_cyp_nom_destino,
waprp029_can_instr												ds_cyp_can_instr,
waprp029_can_dai												ds_cyp_can_dai,
waprp029_cod_est_pieza											cod_cyp_est_pieza,
war04.partition_date

from   bi_corp_staging.arce_warce004 war04
left join bi_corp_staging.aprp_waprp029 wap029 on (war04.warce004_ide_pgo=wap029.waprp029_ide_pgo and wap029.partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='working_day', dag_id='LOAD_CMN_COBROSYPAGOS') }}')
where war04.partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='working_day', dag_id='LOAD_CMN_COBROSYPAGOS') }}' ;