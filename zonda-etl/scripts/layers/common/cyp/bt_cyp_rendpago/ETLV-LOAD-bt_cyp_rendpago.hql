set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
SET mapred.job.queue.name=root.dataeng;


INSERT OVERWRITE TABLE bi_corp_common.bt_cyp_rendpago
PARTITION(partition_date)


select
waprp057_nro_organismo												cod_cyp_nro_organismo,
waprp057_nro_rend													cod_cyp_nro_rend,
waprp057_tpo_reg													ds_cyp_tpo_reg,
waprp057_ide_reg													ds_cyp_ide_resgistro,
waprp057_sec_ide_reg												ds_cyp_sec_registo,
CASE WHEN CAST(waprp057_fec_rend as int)=0
	 THEN NULL ELSE
	 CONCAT(SUBSTRING(waprp057_fec_rend,1,4 ),'-',
SUBSTRING(waprp057_fec_rend ,5,2),'-',
SUBSTRING(waprp057_fec_rend,7,2)) END 				dt_cyp_fecha,
CASE WHEN TRIM(waprp057_raz_soc)=''
	 THEN NULL ELSE TRIM(waprp057_raz_soc) END 													ds_cyp_raz_soc,
waprp057_can_reg													ds_cyp_can_reg,
waprp057_cant_pgos_rend												ds_cyp_can_pgosrend,
waprp057_tot_imp_rend												fc_cyp_tot_imp_rend,
waprp057_acum_com_devg												fc_cyp_acum_comdevg,
waprp057_tot_imp_com_cob											fc_cyp_tot_impcomcob,
waprp057_tot_imp_com_devg											fc_cyp_tot_impcomdvg,
CASE WHEN TRIM(waprp057_nro_clte)=''
	 THEN NULL ELSE TRIM(waprp057_nro_clte) END 					ds_cyp_nro_clte,
CASE WHEN TRIM(waprp057_nom_clte)=''
	 THEN NULL ELSE TRIM(waprp057_nom_clte) END 					ds_cyp_nom_clte,
CASE WHEN waprp057_nro_cuit_clte= 0
	 THEN NULL ELSE waprp057_nro_cuit_clte END 						ds_cyp_cuit,
IF(CAST(waprp057_fec_pgo AS INT)=0 AND CAST(waprp057_hra_pgo AS INT)=0, NULL,
CONCAT(SUBSTRING(waprp057_fec_pgo,1,4 ),'-',
SUBSTRING(waprp057_fec_pgo,5,2),'-',
SUBSTRING(waprp057_fec_pgo,7,2),' ',
SUBSTRING(waprp057_hra_pgo,3,2),':',
SUBSTRING(waprp057_hra_pgo,5,2),':',
SUBSTRING(waprp057_hra_pgo,7,2)))					                ts_cyp_fecha_pago,
waprp057_can_doc													ds_cyp_cant_doc,
waprp057_can_doc_pdtes_acr											ds_cyp_cant_docpdtes,
waprp057_tot_pgo    												fc_cyp_tot_pgo,
waprp057_imp_com_empr												fc_cyp_imp_copempr,
waprp057_imp_cotiz_dl_vend											fc_cyp_imp_cotizvend,
waprp057_imp_cotiz_dl_comp											fc_cyp_imp_cotizcomp,
waprp057_cant_retencion												ds_cyp_cant_rentencion,
waprp057_cant_liquidacion											ds_cyp_cant_liquidacion,
IF(CAST(waprp057_fec_alta_lote AS INT)=0 or waprp057_fec_alta_lote='00010101', NULL,
CONCAT(SUBSTRING(waprp057_fec_alta_lote,1,4 ),'-',
SUBSTRING(waprp057_fec_alta_lote,5,2),'-',
SUBSTRING(waprp057_fec_alta_lote,7,2)))				dt_cyp_fecha_altalote,
waprp057_nro_envio													ds_cyp_nro_envio,
CASE WHEN TRIM(waprp057_nro_orig_inf)=''
	 THEN NULL ELSE TRIM(waprp057_nro_orig_inf) END 				ds_cyp_nro_originf,
waprp057_tot_imp_puni												fc_cyp_tot_imppuni,
waprp057_tot_bonf													fc_cyp_tot_bonf,
CASE WHEN TRIM(waprp057_cod_canal)=''
	 THEN NULL ELSE TRIM(waprp057_cod_canal) END  					cod_cyp_canal,
CASE WHEN TRIM(waprp057_cod_sub_canal)=''
	 THEN NULL ELSE TRIM(waprp057_cod_sub_canal) END 				cod_cyp_subcanal,
CASE WHEN TRIM(waprp057_des_form_pgo)=''
	 THEN NULL ELSE TRIM(waprp057_des_form_pgo) END  				ds_cyp_formapago,
CASE WHEN TRIM(waprp057_nro_instr)=''
	 THEN NULL ELSE TRIM(waprp057_nro_instr) END 					ds_cyp_nro_instrumento,
CASE WHEN waprp057_imp= 0
	 THEN NULL ELSE waprp057_imp END 								fc_cyp_imp,
IF(CAST(waprp057_fec_acred AS INT)=0 or waprp057_fec_acred='00010101' , NULL,
CONCAT(SUBSTRING(waprp057_fec_acred,1,4 ),'-',
SUBSTRING(waprp057_fec_acred,5,2),'-',
SUBSTRING(waprp057_fec_acred,7,2)))					dt_cyp_fec_acred ,
CASE WHEN TRIM(waprp057_mar_acreditacion)=''
	 THEN NULL ELSE TRIM(waprp057_mar_acreditacion) END  			ds_cyp_mar_acreditacion,
IF(CAST(waprp057_fec_vto_cpd AS INT)=0 or waprp057_fec_vto_cpd='00010101' , NULL,
CONCAT(SUBSTRING(waprp057_fec_vto_cpd,1,4 ),'-',
SUBSTRING(waprp057_fec_vto_cpd,5,2),'-',
SUBSTRING(waprp057_fec_vto_cpd,7,2))) 				dt_cyp_fec_vtocpd ,
waprp057_cod_no_a_la_orden											cod_cyp_no_alaorden,
CASE WHEN TRIM(waprp057_mar_confirming)=''
	 THEN NULL ELSE TRIM(waprp057_mar_confirming) END 				ds_cyp_mar_confirming,
CASE WHEN TRIM(waprp057_mar_float)=''
	 THEN NULL ELSE TRIM(waprp057_mar_float) END 					ds_cyp_mar_float,
CASE WHEN TRIM(waprp057_cod_est_instr)=''
	 THEN NULL ELSE TRIM(waprp057_cod_est_instr) END 			    cod_cyp_est_instr,
IF(CAST(waprp057_fec_emision AS INT)=0 or waprp057_fec_emision='00010101', NULL,
CONCAT(SUBSTRING(waprp057_fec_emision,1,4 ),'-',
SUBSTRING(waprp057_fec_emision,5,2),'-',
SUBSTRING(waprp057_fec_emision,7,2)))				dt_cyp_fec_emision,
waprp057_nro_emision												ds_cyp_nro_emision,
CAST(waprp057_nro_suc_dist AS INT)									cod_cyp_nro_sucursaldist,
CASE WHEN TRIM(waprp057_tpo_dist)=''
	 THEN NULL ELSE TRIM(waprp057_tpo_dist) END 					ds_cyp_tipo_dist,
IF(CAST(waprp057_fec_deb AS INT)=0 or waprp057_fec_deb='00010101', NULL,
CONCAT(SUBSTRING(waprp057_fec_deb,1,4 ),'-',
SUBSTRING(waprp057_fec_deb,5,2),'-',
SUBSTRING(waprp057_fec_deb,7,2)))					dt_cyp_fec_deb,
waprp057_cod_pais_cd												cod_cyp_pais_cd,
waprp057_cod_mone_cd												cod_cyp_mone_cd,
waprp057_cod_ent_cbu1_cd											cod_cyp_ent_cbu1,
waprp057_cod_suc_cbu1_cd											cod_cyp_suc_cbu1,
waprp057_nro_dv_cbu1_cd												ds_cyp_nro_dvcbu1,
waprp057_nro_fij_cbu2_cd											ds_cyp_nro_fijcbu2,
waprp057_tpo_cta_cbu2_cd											ds_cyp_tpo_ctacbu2,
waprp057_cod_mone_cbu2_cd											cod_cyp_mone_cbu2,
waprp057_nro_cta_cbu2_cd											ds_cyp_nro_ctacbu2,
waprp057_nro_dv_cbu2_cd												ds_cyp_nro_dvcbu2,
CASE WHEN TRIM(waprp057_cod_form_pgo)=''
	 THEN NULL ELSE TRIM(waprp057_cod_form_pgo) END 												cod_cyp_formapago,
CASE WHEN TRIM(waprp057_cod_mot_rechazo)=''
	 THEN NULL ELSE TRIM(waprp057_cod_mot_rechazo) END 											cod_cyp_mot_rechazo,
CASE WHEN TRIM(waprp057_tpo_cprb)=''
	 THEN NULL ELSE TRIM(waprp057_tpo_cprb) END 													ds_cyp_tpo_cprb,
CASE WHEN TRIM(waprp057_nro_cprb)=''
	 THEN NULL ELSE TRIM(waprp057_nro_cprb) END 													ds_cyp_nro_cprb,
CASE WHEN TRIM(waprp057_nro_cuo)=''
	 THEN NULL ELSE TRIM(waprp057_nro_cuo) END 													ds_cyp_nro_cuo,
IF(CAST(waprp057_fec_vto AS INT)=0 or waprp057_fec_vto='00010101', NULL,
CONCAT(SUBSTRING(waprp057_fec_vto,1,4 ),'-',
SUBSTRING(waprp057_fec_vto,5,2),'-',
SUBSTRING(waprp057_fec_vto,7,2)))					dt_cyp_fecha_vto,
waprp057_tsa_puni													ds_cyp_tsa_puni,
waprp057_imp_deuda													fc_cyp_imp_deuda,
waprp057_imp_puni													fc_cyp_imp_puni,
waprp057_imp_dto													fc_cyp_imp_dto,
waprp057_imp_pgo													fc_cyp_imp_pgo,
CASE WHEN TRIM(waprp057_obs_libr_1er)=''
	 THEN NULL ELSE TRIM(waprp057_obs_libr_1er) END  				ds_cyp_obs_libre1,
CASE WHEN TRIM(waprp057_obs_libr_2da)=''
	 THEN NULL ELSE TRIM(waprp057_obs_libr_2da) END 				ds_cyp_obs_libre2,
CASE WHEN TRIM(waprp057_obs_libr_3ra)=''
	 THEN NULL ELSE TRIM(waprp057_obs_libr_3ra) END  				ds_cyp_obs_libre3,
waprp057_cod_cpto													cod_cyp_cpto,
waprp057_nro_sec_cpto												ds_cyp_nro_seccpto,
waprp057_imp_cotiz_dl_v_ca											fc_cyp_imp_cotizdlvca,
waprp057_imp_cotiz_dl_c_ca											fc_cyp_imp_cotizdlcca,
waprp057_imp_cpto													fc_cyp_imp_cpto,
waprp057_imp_cpto_discr												fc_cyp_imp_cptodiscr,
waprp057_imp_iva													fc_cyp_imp_iva,
waprp057_imp_iva_adic												fc_cyp_imp_ivaadic,
waprp057_imp_ing_bru												fc_cyp_imp_ingbru,
IF(CAST(waprp057_fec_movim AS INT)=0 or waprp057_fec_movim='00010101', NULL,
CONCAT(SUBSTRING(waprp057_fec_movim,1,4 ),'-',
SUBSTRING(waprp057_fec_movim,5,2),'-',
SUBSTRING(waprp057_fec_movim,7,2)))					dt_cyp_fec_movim,
CASE WHEN TRIM(waprp057_tipo_docu)=''
	 THEN NULL ELSE TRIM(waprp057_tipo_docu) END 					ds_cyp_tipo_docu,
waprp057_nro_docu					ds_cyp_nro_docu,
CASE WHEN TRIM(waprp057_tipo_docu_1)=''
	 THEN NULL ELSE TRIM(waprp057_tipo_docu_1) END 					ds_cyp_tipo_docu1,
waprp057_nro_docu_1					ds_cyp_nro_docu1,
CASE WHEN TRIM(waprp057_tipo_docu_2)=''
	 THEN NULL ELSE TRIM(waprp057_tipo_docu_2) END 					ds_cyp_tipo_docu2,
waprp057_nro_docu_2													ds_cyp_nro_docu2,
waprp057_suc_dist													cod_cyp_sucursal_dist,
CASE WHEN TRIM(waprp057_tpo_recibo)=''
	 THEN NULL ELSE TRIM(waprp057_tpo_recibo) END 	 				ds_cyp_tpo_recibo,
waprp057_nro_secu_dom												ds_cyp_nro_secudom,
partition_Date

from  bi_corp_staging.aprp_waprp057
where partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='working_day', dag_id='LOAD_CMN_COBROSYPAGOS') }}'
AND  '{{ ti.xcom_pull(task_ids='InputConfig', key='working_day', dag_id='LOAD_CMN_COBROSYPAGOS') }}'>='2021-02-01'

union all

select
CAST(nro_organismo AS BIGINT)							cod_cyp_nro_organismo,
CAST(nro_rend AS INT)									cod_cyp_nro_rend,
tpo_reg													ds_cyp_tpo_reg,
ide_reg													ds_cyp_ide_resgistro,
CAST(sec_ide_reg AS INT)								ds_cyp_sec_registo,
CASE WHEN fec_rend='null'
	 THEN NULL ELSE SUBSTRING(fec_rend,1,10) END 		dt_cyp_fecha,

CASE WHEN raz_soc='null'
	 THEN NULL ELSE raz_soc END 						ds_cyp_raz_soc,
CASE WHEN can_reg='null'
	 THEN NULL ELSE CAST(can_reg AS BIGINT) END 		ds_cyp_can_reg,
CASE WHEN cant_pgos_rend='null'
	 THEN NULL ELSE CAST(cant_pgos_rend AS BIGINT) END 	ds_cyp_can_pgosrend,
CASE WHEN tot_imp_rend='null'
	 THEN NULL ELSE CAST(tot_imp_rend AS decimal(15,2)) END fc_cyp_tot_imp_rend,
CASE WHEN acum_com_devg='null'
	 THEN NULL ELSE CAST(acum_com_devg AS decimal(15,2)) END fc_cyp_acum_comdevg,
CASE WHEN tot_imp_com_cob='null'
	 THEN NULL ELSE CAST(tot_imp_com_cob AS decimal(15,2)) END fc_cyp_tot_impcomcob,
CASE WHEN tot_imp_com_devg='null'
	 THEN NULL ELSE CAST(tot_imp_com_devg AS decimal(15,2)) END fc_cyp_tot_impcomdvg,
CASE WHEN nro_clte='null'
	 THEN NULL ELSE nro_clte END 							ds_cyp_nro_clte,
CASE WHEN nom_clte='null'
	 THEN NULL ELSE nom_clte END 							ds_cyp_nom_clte,
CASE WHEN nro_cuit_clte='null'
	 THEN NULL ELSE CAST(nro_cuit_clte AS BIGINT) END 		ds_cyp_cuit,
CASE WHEN fec_pgo='null' AND hra_pgo='null'
	 THEN NULL ELSE
	 CONCAT(SUBSTRING(fec_pgo,1,10), ' ',
	 CONCAT(SUBSTRING(hra_pgo,1,2),':',
	 SUBSTRING(hra_pgo,3,2),':',
	 SUBSTRING(hra_pgo,5,2))) END 							ts_cyp_fecha_pago,
CASE WHEN can_doc='null'
	 THEN NULL ELSE cast(can_doc as int) END 				ds_cyp_cant_doc,
CASE WHEN can_doc_pdtes_acr='null'
	 THEN NULL ELSE cast(can_doc_pdtes_acr as int) END 		ds_cyp_cant_docpdtes,
CASE WHEN tot_pgo='null'
	 THEN NULL ELSE CAST(tot_pgo AS decimal(15,2)) END 		fc_cyp_tot_pgo,
CASE WHEN imp_com_empr='null'
	 THEN NULL ELSE CAST(imp_com_empr AS decimal(15,2)) END fc_cyp_imp_copempr,
CASE WHEN imp_cotiz_dl_vend='null'
	 THEN NULL ELSE CAST(imp_cotiz_dl_vend AS decimal(15,2)) END fc_cyp_imp_cotizvend,
CASE WHEN imp_cotiz_dl_comp='null'
	 THEN NULL ELSE CAST(imp_cotiz_dl_comp AS decimal(15,2)) END fc_cyp_imp_cotizcomp,
CASE WHEN cant_retencion='null'
	 THEN NULL ELSE cast(cant_retencion as bigint) END 			ds_cyp_cant_rentencion,
CASE WHEN cant_liquidacion='null'
	 THEN NULL ELSE cast(cant_liquidacion as bigint) END 		ds_cyp_cant_liquidacion,
CASE WHEN fec_alta_lote='null' OR
		 SUBSTRING(fec_alta_lote,1,10)='0001-01-01'
	 THEN NULL ELSE SUBSTRING(fec_alta_lote,1,10) END 							dt_cyp_fecha_altalote,
CASE WHEN nro_envio='null'
	 THEN NULL ELSE cast(nro_envio as int) END 					ds_cyp_nro_envio,
CASE WHEN nro_orig_inf='null'
	 THEN NULL ELSE nro_orig_inf END 							ds_cyp_nro_originf,
CASE WHEN tot_imp_puni='null'
	 THEN NULL ELSE CAST(tot_imp_puni AS decimal(15,2)) END 	fc_cyp_tot_imppuni,
CASE WHEN tot_bonf='null'
	 THEN NULL ELSE CAST(tot_bonf AS decimal(15,2)) END 		fc_cyp_tot_bonf,
CASE WHEN cod_canal='null'
	 THEN NULL ELSE cod_canal END 								cod_cyp_canal,
CASE WHEN cod_sub_canal='null'
	 THEN NULL ELSE cod_sub_canal END 							cod_cyp_subcanal,
CASE WHEN des_form_pgo='null'
	 THEN NULL ELSE des_form_pgo END 							ds_cyp_formapago,
CASE WHEN nro_instr='null'
	 THEN NULL ELSE nro_instr END 								ds_cyp_nro_instrumento,
CASE WHEN imp='null'
	 THEN NULL ELSE cast(imp as decimal(15,2)) END 				fc_cyp_imp,
CASE WHEN fec_acred='null' OR
		 SUBSTRING(fec_acred,1,10)='0001-01-01'
	 THEN NULL ELSE SUBSTRING(fec_acred,1,10) END 				dt_cyp_fec_acred,
CASE WHEN mar_acreditacion='null'
	 THEN NULL ELSE mar_acreditacion END 						ds_cyp_mar_acreditacion,
CASE WHEN fec_vto_cpd='null'  OR
		 SUBSTRING(fec_vto_cpd,1,10)='0001-01-01'
	 THEN NULL ELSE SUBSTRING(fec_vto_cpd,1,10) END 			dt_cyp_fec_vtocpd,
CASE WHEN cod_no_a_la_orden='null'
	 THEN NULL ELSE cast(cod_no_a_la_orden as int) END 						cod_cyp_no_alaorden,
CASE WHEN mar_confirming='null'
	 THEN NULL ELSE mar_confirming END 							ds_cyp_mar_confirming,
CASE WHEN mar_float='null'
	 THEN NULL ELSE mar_float END 								ds_cyp_mar_float,
CASE WHEN cod_est_instr='null'
	 THEN NULL ELSE cod_est_instr END 							cod_cyp_est_instr,
CASE WHEN fec_emision='null' OR
		 SUBSTRING(fec_emision,1,10)='0001-01-01'
	 THEN NULL ELSE SUBSTRING(fec_emision,1,10) END 			dt_cyp_fec_emision,
CASE WHEN nro_emision='null'
	 THEN NULL ELSE cast(nro_emision as int) END 				ds_cyp_nro_emision,
CASE WHEN nro_suc_dist='null'
	 THEN NULL ELSE cast(nro_suc_dist as int) END 				cod_cyp_nro_sucursaldist,
CASE WHEN tpo_dist='null'
	 THEN NULL ELSE tpo_dist END 								ds_cyp_tipo_dist,
CASE WHEN fec_deb='null' OR
		 SUBSTRING(fec_deb,1,10)='0001-01-01'
	 THEN NULL ELSE SUBSTRING(fec_deb,1,10) END 				dt_cyp_fec_deb,
CASE WHEN cod_pais_cd='null'
	 THEN NULL ELSE cast(cod_pais_cd as int) END 				cod_cyp_pais_cd,
CASE WHEN cod_mone_cd='null'
	 THEN NULL ELSE cast(cod_mone_cd as int) END 				cod_cyp_mone_cd,
CASE WHEN cod_ent_cbu1_cd='null'
	 THEN NULL ELSE cast(cod_ent_cbu1_cd as int) END 			cod_cyp_ent_cbu1,
CASE WHEN cod_suc_cbu1_cd='null'
	 THEN NULL ELSE  cast(cod_suc_cbu1_cd as int) END 			cod_cyp_suc_cbu1,
CASE WHEN nro_dv_cbu1_cd='null'
	 THEN NULL ELSE cast(nro_dv_cbu1_cd as int) END 			ds_cyp_nro_dvcbu1,
CASE WHEN nro_fij_cbu2_cd='null'
	 THEN NULL ELSE cast(nro_fij_cbu2_cd as int) END 			ds_cyp_nro_fijcbu2,
CASE WHEN tpo_cta_cbu2_cd='null'
	 THEN NULL ELSE cast(tpo_cta_cbu2_cd as int) END 			ds_cyp_tpo_ctacbu2,
CASE WHEN cod_mone_cbu2_cd='null'
	 THEN NULL ELSE cast(cod_mone_cbu2_cd as int) END 			cod_cyp_mone_cbu2,
CASE WHEN nro_cta_cbu2_cd='null'
	 THEN NULL ELSE cast(nro_cta_cbu2_cd as bigint) END 		ds_cyp_nro_ctacbu2,
CASE WHEN nro_dv_cbu2_cd='null'
	 THEN NULL ELSE cast(nro_dv_cbu2_cd as int) END 			ds_cyp_nro_dvcbu2,
CASE WHEN cod_form_pgo='null'
	 THEN NULL ELSE cod_form_pgo END 							cod_cyp_formapago,
CASE WHEN cod_mot_rechazo='null'
	 THEN NULL ELSE cod_mot_rechazo END							cod_cyp_mot_rechazo,
CASE WHEN tpo_cprb='null'
	 THEN NULL ELSE tpo_cprb END 								ds_cyp_tpo_cprb,
CASE WHEN nro_cprb='null'
	 THEN NULL ELSE nro_cprb END 								ds_cyp_nro_cprb,
CASE WHEN nro_cuo='null'
	 THEN NULL ELSE nro_cuo END 								ds_cyp_nro_cuo,
CASE WHEN fec_vto='null' OR
		 SUBSTRING(fec_vto,1,10)='0001-01-01'
	 THEN NULL ELSE SUBSTRING(fec_vto,1,10) END 				dt_cyp_fecha_vto,
CASE WHEN tsa_puni='null'
	 THEN NULL ELSE CAST(tsa_puni AS decimal(5,2)) END 			ds_cyp_tsa_puni,
CASE WHEN imp_deuda='null'
	 THEN NULL ELSE CAST(imp_deuda AS decimal(15,2))  END 		fc_cyp_imp_deuda,
CASE WHEN imp_puni='null'
	 THEN NULL ELSE CAST(imp_puni AS decimal(15,2)) END 		fc_cyp_imp_puni,
CASE WHEN imp_dto='null'
	 THEN NULL ELSE CAST(imp_dto AS decimal(15,2)) END 			fc_cyp_imp_dto,
CASE WHEN imp_pgo='null'
	 THEN NULL ELSE  CAST(imp_pgo AS decimal(15,2)) END 		fc_cyp_imp_pgo,
CASE WHEN obs_libr_1er='null'
	 THEN NULL ELSE obs_libr_1er END 							ds_cyp_obs_libre1,
CASE WHEN obs_libr_2da='null'
	 THEN NULL ELSE obs_libr_2da END 							ds_cyp_obs_libre2,
CASE WHEN obs_libr_3ra='null'
	 THEN NULL ELSE obs_libr_3ra END 							ds_cyp_obs_libre3,
CASE WHEN cod_cpto='null'
	 THEN NULL ELSE cast(cod_cpto as int) END 					cod_cyp_cpto,
CASE WHEN nro_sec_cpto='null'
	 THEN NULL ELSE cast(nro_sec_cpto as int) END 				ds_cyp_nro_seccpto,
CASE WHEN imp_cotiz_dl_v_ca='null'
	 THEN NULL ELSE cast(imp_cotiz_dl_v_ca as decimal(15,2)) END fc_cyp_imp_cotizdlvca,
CASE WHEN imp_cotiz_dl_c_ca='null'
	 THEN NULL ELSE cast(imp_cotiz_dl_c_ca as decimal(15,2)) END fc_cyp_imp_cotizdlcca,
CASE WHEN imp_cpto='null'
	 THEN NULL ELSE cast(imp_cpto as decimal(15,2)) END 		fc_cyp_imp_cpto,
CASE WHEN imp_cpto_discr='null'
	 THEN NULL ELSE cast(imp_cpto_discr as decimal(15,2)) END 	fc_cyp_imp_cptodiscr,
CASE WHEN imp_iva='null'
	 THEN NULL ELSE cast(imp_iva as decimal(15,2)) END 			fc_cyp_imp_iva,
CASE WHEN imp_iva_adic='null'
	 THEN NULL ELSE cast(imp_iva_adic as decimal(15,2)) END 	fc_cyp_imp_ivaadic,
CASE WHEN imp_ing_bru='null'
	 THEN NULL ELSE cast(imp_ing_bru as decimal(15,2)) END 		fc_cyp_imp_ingbru,
CASE WHEN fec_movim='null' OR
		 SUBSTRING(fec_movim,1,10)='0001-01-01'
	 THEN NULL ELSE SUBSTRING(fec_movim,1,10) END 				dt_cyp_fec_movim,
CASE WHEN tipo_docu='null'
	 THEN NULL ELSE tipo_docu END 								ds_cyp_tipo_docu,
CASE WHEN nro_docu='null'
	 THEN NULL ELSE cast(nro_docu as bigint) END 								ds_cyp_nro_docu,
CASE WHEN tipo_docu_1='null'
	 THEN NULL ELSE tipo_docu_1 END 							ds_cyp_tipo_docu1,
CASE WHEN nro_docu_1='null'
	 THEN NULL ELSE cast(nro_docu_1 as bigint) END 				ds_cyp_nro_docu1,
CASE WHEN tipo_docu_2='null'
	 THEN NULL ELSE tipo_docu_2 END 							ds_cyp_tipo_docu2,
CASE WHEN nro_docu_2='null'
	 THEN NULL ELSE cast(nro_docu_2 as bigint) END 				ds_cyp_nro_docu2,
CASE WHEN suc_dist='null'
	 THEN NULL ELSE cast(suc_dist as int) END 					cod_cyp_sucursal_dist,
CASE WHEN tpo_recibo='null'
	 THEN NULL ELSE tpo_recibo END 								ds_cyp_tpo_recibo,
CASE WHEN nro_secu_dom='null'
	 THEN NULL ELSE cast(nro_secu_dom as int) END 				ds_cyp_nro_secudom,
partition_date

from  bi_corp_staging.rio143_big_tprp_rend_pago
WHERE partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='working_day', dag_id='LOAD_CMN_COBROSYPAGOS') }}'
AND '{{ ti.xcom_pull(task_ids='InputConfig', key='working_day', dag_id='LOAD_CMN_COBROSYPAGOS') }}' <'2021-02-01'
AND '{{ ti.xcom_pull(task_ids='InputConfig', key='working_day', dag_id='LOAD_CMN_COBROSYPAGOS') }}'>='2019-01-01';





