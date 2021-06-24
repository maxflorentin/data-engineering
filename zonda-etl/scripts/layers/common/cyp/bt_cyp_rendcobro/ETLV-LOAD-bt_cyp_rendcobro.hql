set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
SET mapred.job.queue.name=root.dataeng;


INSERT OVERWRITE TABLE bi_corp_common.bt_cyp_rendcobro
PARTITION(partition_date)



select
warce057_nro_organismo												cod_cyp_nro_organismo,
warce057_nro_rend                   								cod_cyp_nro_rend,
warce057_tpo_reg                    								ds_cyp_tpo_reg,
warce057_ide_reg                    								ds_cyp_ide_resgistro,
warce057_sec_ide_reg                								ds_cyp_sec_registo,
IF(CAST(warce057_fec_rend AS INT)=0, NULL,
CONCAT(SUBSTRING(warce057_fec_rend,1,4 ),'-',
SUBSTRING(warce057_fec_rend,5,2),'-',
SUBSTRING(warce057_fec_rend,7,2)))					                dt_cyp_fecha                   ,
CASE WHEN TRIM(warce057_raz_soc)=''
	 THEN NULL ELSE TRIM(warce057_raz_soc) END						 ds_cyp_raz_soc,
warce057_cod_pais_cr                								cod_cyp_cta_recau,
warce057_cod_mone_cr                								cod_cyp_moneda,
warce057_cod_ent_cbu1_cr            								cod_cyp_ent_cbu1,
warce057_cod_suc_cbu1_cr            								cod_cyp_suc_cr,
warce057_nro_dv_cbu1_cr             								ds_cyp_nro_dvcbu1,
warce057_nro_fij_cbu2_cr           									ds_cyp_nro_fijcbu2,
warce057_tpo_cta_cbu2_cr            								ds_cyp_tpo_ctacbu2,
warce057_cod_mone_cbu2_cr           								cod_cyp_moneda_cbu2,
warce057_nro_cta_cbu2_cr            								cod_cyp_nro_ctacbu2,
warce057_nro_dv_cbu2_cr             								cod_cyp_dv_cbu2,
warce057_nro_arec                   								ds_cyp_nro_arec,
warce057_can_reg                    								ds_cyp_can_reg,
warce057_cant_pgos_rend             								ds_cyp_can_pgosrend,
warce057_tot_imp_rend               								fc_cyp_tot_imp_rend,
warce057_acum_com_devg              								fc_cyp_acum_comdevg,
warce057_tot_imp_com_cob            								fc_cyp_tot_impcomcob,
warce057_tot_imp_com_devg           							    fc_cyp_tot_impcomdvg,
CASE WHEN TRIM(warce057_nro_clte)=''
	 THEN NULL ELSE TRIM(warce057_nro_clte) END	                   								ds_cyp_nro_clte,
CASE WHEN TRIM(warce057_nom_clte)=''
	 THEN NULL ELSE TRIM(warce057_nom_clte) END	                   								ds_cyp_nom_clte,
CASE WHEN warce057_nro_cuit_clte=0
	 THEN NULL ELSE warce057_nro_cuit_clte END	              														ds_cyp_cuit,
IF(CAST(warce057_fec_pgo AS INT)=0 AND CAST(warce057_hra_pgo AS INT)=0, NULL,
CONCAT(SUBSTRING(warce057_fec_pgo,1,4 ),'-',
SUBSTRING(warce057_fec_pgo,5,2),'-',
SUBSTRING(warce057_fec_pgo,7,2),' ',
SUBSTRING(warce057_hra_pgo,3,2),':',
SUBSTRING(warce057_hra_pgo,5,2),':',
SUBSTRING(warce057_hra_pgo,7,2)))					                ts_cyp_fecha_pago,
warce057_nro_bco_corrs              								ds_cyp_bco_corrs,
warce057_can_doc                    								ds_cyp_cant_doc,
warce057_can_doc_pdtes_acr          								ds_cyp_cant_docpdtes,
warce057_tot_pgo                    								fc_cyp_tot_pgo,
warce057_imp_com_empr               								fc_cyp_imp_copempr,
warce057_imp_com_3ra                								fc_cyp_imp_cop3ra,
warce057_imp_cotiz_dl_vend          								fc_cyp_imp_cotizvend,
warce057_imp_cotiz_dl_comp          								fc_cyp_imp_cotizcomp,
cast(warce057_suc_orig as int)                   					cod_cyp_sucursal,
CASE WHEN trim(warce057_nro_boleta)=''
	 THEN NULL ELSE trim(warce057_nro_boleta) END 					ds_cyp_nro_boleta,
CASE WHEN trim(warce057_cod_trn)=''
	 THEN NULL ELSE trim(warce057_cod_trn) END                     	ds_cyp_trn,
CASE WHEN trim(warce057_motivo_ajuste)=''
	 THEN NULL ELSE trim(warce057_motivo_ajuste) END               	ds_cyp_motivo_ajuste,
CASE WHEN trim(warce057_signo_ajuste)=''
	 THEN NULL ELSE trim(warce057_signo_ajuste) END               	ds_cyp_signo_ajuste,
warce057_imp_ajuste                 								fc_cyp_imp_ajuste,
CASE WHEN trim(warce057_cod_captu_caja)=''
	 THEN NULL ELSE trim(warce057_cod_captu_caja) END              	cod_cyp_captu_caja,
CASE WHEN trim(warce057_nro_suc_orig_canl)=''
	 THEN NULL ELSE CAST(warce057_nro_suc_orig_canl AS INT) END     cod_cyp_sucursal_orig,
CASE WHEN trim(warce057_cod_canal)=''
	 THEN NULL ELSE trim(warce057_cod_canal) END                    cod_cyp_canal,
CASE WHEN trim(warce057_cod_sub_canal)=''
	 THEN NULL ELSE trim(warce057_cod_sub_canal) END               	cod_cyp_subcanal,
CASE WHEN trim(warce057_des_form_pgo)=''
	 THEN NULL ELSE trim(warce057_des_form_pgo) END                 ds_cyp_formapago,
CASE WHEN trim(warce057_nro_instrumento)=''
	 THEN NULL ELSE trim(warce057_nro_instrumento) END             	ds_cyp_nro_instrumento,
warce057_imp                        								fc_cyp_imp,
IF(CAST(warce057_fec_acred AS INT)=0, NULL,
CONCAT(SUBSTRING(warce057_fec_acred,1,4 ),'-',
SUBSTRING(warce057_fec_acred,5,2),'-',
SUBSTRING(warce057_fec_acred,7,2)))	                                    dt_cyp_fec_acred ,
CASE WHEN trim(warce057_mar_acreditacion)=''
	 THEN NULL ELSE trim(warce057_mar_acreditacion) END            	 ds_cyp_mar_acreditacion,
IF(CAST(warce057_fec_vto_cpd AS INT)=0, NULL,
CONCAT(SUBSTRING(warce057_fec_vto_cpd,1,4 ),'-',
SUBSTRING(warce057_fec_vto_cpd,5,2),'-',
SUBSTRING(warce057_fec_vto_cpd,7,2)))	             dt_cyp_fec_vtocpd ,
warce057_cod_form_pgo               								 cod_cyp_forma_pgo,
CASE WHEN trim(warce057_cod_mot_rechazo)=''
	 THEN NULL ELSE trim(warce057_cod_mot_rechazo) END             	 cod_cyp_mot_rechazo,
CASE WHEN trim(warce057_cod_mot_rend)=''
	 THEN NULL ELSE trim(warce057_cod_mot_rend) END             	 cod_cyp_mot_rend,
warce057_nro_sec_movim              								 ds_cyp_sec_movim,
warce057_can_otr_impre              								 ds_cyp_can_otrimpre,
CASE WHEN trim(warce057_tpo_cprb)=''
	 THEN NULL ELSE trim(warce057_tpo_cprb) END                    	 ds_cyp_tpo_cprb,
CASE WHEN trim(warce057_nro_cprb)=''
	 THEN NULL ELSE trim(warce057_nro_cprb) END                      ds_cyp_nro_cprb,
CASE WHEN trim(warce057_nro_cuo)=''
	 THEN NULL ELSE trim(warce057_nro_cuo) END                       ds_cyp_nro_cuo,
IF(CAST(warce057_fec_vto AS INT)=0, NULL,
CONCAT(SUBSTRING(warce057_fec_vto,1,4 ),'-',
SUBSTRING(warce057_fec_vto,5,2),'-',
SUBSTRING(warce057_fec_vto,7,2)))                                     dt_cyp_fecha_vto,
warce057_tsa_puni                   								  ds_cyp_tsa_puni,
warce057_imp_deuda                  								  fc_cyp_imp_deuda,
warce057_imp_puni                   								  fc_cyp_imp_puni,
warce057_imp_dto                    								  fc_cyp_imp_dto,
warce057_imp_pgo                    								  fc_cyp_imp_pgo,
CASE WHEN trim(warce057_obs_libr_1er)=''
	 THEN NULL ELSE trim(warce057_obs_libr_1er) END                								  ds_cyp_obs_libre1,
CASE WHEN trim(warce057_obs_libr_2da)=''
	 THEN NULL ELSE trim(warce057_obs_libr_2da) END                								  ds_cyp_obs_libre2,
CASE WHEN trim(warce057_obs_libr_3ra)=''
	 THEN NULL ELSE trim(warce057_obs_libr_3ra) END                                                  ds_cyp_obs_libre3,
partition_date
from  bi_corp_staging.arce_warce057
WHERE partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='working_day', dag_id='LOAD_CMN_COBROSYPAGOS') }}'
and '{{ ti.xcom_pull(task_ids='InputConfig', key='working_day', dag_id='LOAD_CMN_COBROSYPAGOS') }}'>='2021-02-01'

union ALL



select
warce057_sice_nro_organismo												cod_cyp_nro_organismo,
warce057_sice_nro_rend                   								cod_cyp_nro_rend,
warce057_sice_tpo_reg                    								ds_cyp_tpo_reg,
warce057_sice_ide_reg                    								ds_cyp_ide_resgistro,
warce057_sice_sec_ide_reg                								ds_cyp_sec_registo,
IF(CAST(warce057_sice_fec_rend AS INT)=0, NULL,
CONCAT(SUBSTRING(warce057_sice_fec_rend,1,4 ),'-',
SUBSTRING(warce057_sice_fec_rend,5,2),'-',
SUBSTRING(warce057_sice_fec_rend,7,2)))					                dt_cyp_fecha                   ,
CASE WHEN TRIM(warce057_sice_raz_soc)=''
	 THEN NULL ELSE TRIM(warce057_sice_raz_soc) END						 ds_cyp_raz_soc,
warce057_sice_cod_pais_cr                								cod_cyp_cta_recau,
warce057_sice_cod_mone_cr                								cod_cyp_moneda,
warce057_sice_cod_ent_cbu1_cr            								cod_cyp_ent_cbu1,
warce057_sice_cod_suc_cbu1_cr            								cod_cyp_suc_cr,
warce057_sice_nro_dv_cbu1_cr             								ds_cyp_nro_dvcbu1,
warce057_sice_nro_fij_cbu2_cr           									ds_cyp_nro_fijcbu2,
warce057_sice_tpo_cta_cbu2_cr            								ds_cyp_tpo_ctacbu2,
warce057_sice_cod_mone_cbu2_cr           								cod_cyp_moneda_cbu2,
warce057_sice_nro_cta_cbu2_cr            								cod_cyp_nro_ctacbu2,
warce057_sice_nro_dv_cbu2_cr             								cod_cyp_dv_cbu2,
warce057_sice_nro_arec                   								ds_cyp_nro_arec,
warce057_sice_can_reg                    								ds_cyp_can_reg,
warce057_sice_cant_pgos_rend             								ds_cyp_can_pgosrend,
warce057_sice_tot_imp_rend               								fc_cyp_tot_imp_rend,
warce057_sice_acum_com_devg              								fc_cyp_acum_comdevg,
warce057_sice_tot_imp_com_cob            								fc_cyp_tot_impcomcob,
warce057_sice_tot_imp_com_devg           							    fc_cyp_tot_impcomdvg,
CASE WHEN TRIM(warce057_sice_nro_clte)=''
	 THEN NULL ELSE TRIM(warce057_sice_nro_clte) END	                   								ds_cyp_nro_clte,
CASE WHEN TRIM(warce057_sice_nom_clte)=''
	 THEN NULL ELSE TRIM(warce057_sice_nom_clte) END	                   								ds_cyp_nom_clte,
CASE WHEN warce057_sice_nro_cuit_clte=0
	 THEN NULL ELSE warce057_sice_nro_cuit_clte END	              														ds_cyp_cuit,
IF(CAST(warce057_sice_fec_pgo AS INT)=0 AND CAST(warce057_sice_hra_pgo AS INT)=0, NULL,
CONCAT(SUBSTRING(warce057_sice_fec_pgo,1,4 ),'-',
SUBSTRING(warce057_sice_fec_pgo,5,2),'-',
SUBSTRING(warce057_sice_fec_pgo,7,2),' ',
SUBSTRING(warce057_sice_hra_pgo,3,2),':',
SUBSTRING(warce057_sice_hra_pgo,5,2),':',
SUBSTRING(warce057_sice_hra_pgo,7,2)))					ts_cyp_fecha_pago,
warce057_sice_nro_bco_corrs              								ds_cyp_bco_corrs,
warce057_sice_can_doc                    								ds_cyp_cant_doc,
warce057_sice_can_doc_pdtes_acr          								ds_cyp_cant_docpdtes,
warce057_sice_tot_pgo                    								fc_cyp_tot_pgo,
warce057_sice_imp_com_empr               								fc_cyp_imp_copempr,
warce057_sice_imp_com_3ra                								fc_cyp_imp_cop3ra,
warce057_sice_imp_cotiz_dl_vend          								fc_cyp_imp_cotizvend,
warce057_sice_imp_cotiz_dl_comp          								fc_cyp_imp_cotizcomp,
cast(warce057_sice_suc_orig as int)                   					cod_cyp_sucursal,
CASE WHEN trim(warce057_sice_nro_boleta)=''
	 THEN NULL ELSE trim(warce057_sice_nro_boleta) END 					ds_cyp_nro_boleta,
CASE WHEN trim(warce057_sice_cod_trn)=''
	 THEN NULL ELSE trim(warce057_sice_cod_trn) END                     	ds_cyp_trn,
CASE WHEN trim(warce057_sice_motivo_ajuste)=''
	 THEN NULL ELSE trim(warce057_sice_motivo_ajuste) END               	ds_cyp_motivo_ajuste,
CASE WHEN trim(warce057_sice_signo_ajuste)=''
	 THEN NULL ELSE trim(warce057_sice_signo_ajuste) END               	ds_cyp_signo_ajuste,
warce057_sice_imp_ajuste                 								fc_cyp_imp_ajuste,
CASE WHEN trim(warce057_sice_cod_captu_caja)=''
	 THEN NULL ELSE trim(warce057_sice_cod_captu_caja) END              	cod_cyp_captu_caja,
CASE WHEN trim(warce057_sice_nro_suc_orig_canl)=''
	 THEN NULL ELSE CAST(warce057_sice_nro_suc_orig_canl AS INT) END     cod_cyp_sucursal_orig,
CASE WHEN trim(warce057_sice_cod_canal)=''
	 THEN NULL ELSE trim(warce057_sice_cod_canal) END                    cod_cyp_canal,
CASE WHEN trim(warce057_sice_cod_sub_canal)=''
	 THEN NULL ELSE trim(warce057_sice_cod_sub_canal) END               	cod_cyp_subcanal,
CASE WHEN trim(warce057_sice_des_form_pgo)=''
	 THEN NULL ELSE trim(warce057_sice_des_form_pgo) END                 ds_cyp_formapago,
CASE WHEN trim(warce057_sice_nro_instrumento)=''
	 THEN NULL ELSE trim(warce057_sice_nro_instrumento) END             	ds_cyp_nro_instrumento,
warce057_sice_imp                        								fc_cyp_imp,
IF(CAST(warce057_sice_fec_acred AS INT)=0, NULL,
CONCAT(SUBSTRING(warce057_sice_fec_acred,1,4 ),'-',
SUBSTRING(warce057_sice_fec_acred,5,2),'-',
SUBSTRING(warce057_sice_fec_acred,7,2)))	                 dt_cyp_fec_acred ,
CASE WHEN trim(warce057_sice_mar_acreditacion)=''
	 THEN NULL ELSE trim(warce057_sice_mar_acreditacion) END            	 ds_cyp_mar_acreditacion,
IF(CAST(warce057_sice_fec_vto_cpd AS INT)=0, NULL,
CONCAT(SUBSTRING(warce057_sice_fec_vto_cpd,1,4 ),'-',
SUBSTRING(warce057_sice_fec_vto_cpd,5,2),'-',
SUBSTRING(warce057_sice_fec_vto_cpd ,7,2)))	             dt_cyp_fec_vtocpd ,
warce057_sice_cod_form_pgo               								 cod_cyp_forma_pgo,
CASE WHEN trim(warce057_sice_cod_mot_rechazo)=''
	 THEN NULL ELSE trim(warce057_sice_cod_mot_rechazo) END             	 cod_cyp_mot_rechazo,
CASE WHEN trim(warce057_sice_cod_mot_rend)=''
	 THEN NULL ELSE trim(warce057_sice_cod_mot_rend) END             	 cod_cyp_mot_rend,
warce057_sice_nro_sec_movim              								 ds_cyp_sec_movim,
warce057_sice_can_otr_impre              								 ds_cyp_can_otrimpre,
CASE WHEN trim(warce057_sice_tpo_cprb)=''
	 THEN NULL ELSE trim(warce057_sice_tpo_cprb) END                    	 ds_cyp_tpo_cprb,
CASE WHEN trim(warce057_sice_nro_cprb)=''
	 THEN NULL ELSE trim(warce057_sice_nro_cprb) END                      ds_cyp_nro_cprb,
CASE WHEN trim(warce057_sice_nro_cuo)=''
	 THEN NULL ELSE trim(warce057_sice_nro_cuo) END                       ds_cyp_nro_cuo,
IF(CAST(warce057_sice_fec_vto AS INT)=0, NULL,
CONCAT(SUBSTRING(warce057_sice_fec_vto,1,4 ),'-',
SUBSTRING(warce057_sice_fec_vto,5,2),'-',
SUBSTRING(warce057_sice_fec_vto,7,2)))                     dt_cyp_fecha_vto,
warce057_sice_tsa_puni                   								  ds_cyp_tsa_puni,
warce057_sice_imp_deuda                  								  fc_cyp_imp_deuda,
warce057_sice_imp_puni                   								  fc_cyp_imp_puni,
warce057_sice_imp_dto                    								  fc_cyp_imp_dto,
warce057_sice_imp_pgo                    								  fc_cyp_imp_pgo,
CASE WHEN trim(warce057_sice_obs_libr_1er)=''
	 THEN NULL ELSE trim(warce057_sice_obs_libr_1er) END                								  ds_cyp_obs_libre1,
CASE WHEN trim(warce057_sice_obs_libr_2da)=''
	 THEN NULL ELSE trim(warce057_sice_obs_libr_2da) END                								  ds_cyp_obs_libre2,
CASE WHEN trim(warce057_sice_obs_libr_3ra)=''
	 THEN NULL ELSE trim(warce057_sice_obs_libr_3ra) END                                                  ds_cyp_obs_libre3,
partition_date
from  bi_corp_staging.arce_warce057_sice
WHERE partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='working_day', dag_id='LOAD_CMN_COBROSYPAGOS') }}'
and '{{ ti.xcom_pull(task_ids='InputConfig', key='working_day', dag_id='LOAD_CMN_COBROSYPAGOS') }}'>='2021-02-01'


union all



select
CAST(nro_organismo AS BIGINT)							cod_cyp_nro_organismo,
CAST(nro_rend AS INT)									cod_cyp_nro_rend,
CASE WHEN TRIM(tpo_reg)='null'
	 THEN NULL ELSE TRIM(tpo_reg) END					ds_cyp_tpo_reg,
CASE WHEN TRIM(ide_reg)='null'
	 THEN NULL ELSE TRIM(ide_reg) END 					ds_cyp_ide_resgistro,
CAST(sec_ide_reg AS INT)								ds_cyp_sec_registo,
CASE WHEN TRIM(fec_rend)='null'
	 THEN NULL ELSE SUBSTRING(fec_rend,1,10) END 		dt_cyp_fecha,
CASE WHEN TRIM(raz_soc)='null'
	 THEN NULL ELSE TRIM(raz_soc) END					ds_cyp_raz_soc,
CAST(cta_recaud AS INT)									cod_cyp_cta_recau,
CAST(cod_mone_cr AS INT)								cod_cyp_moneda,
CAST(cbu1_cr AS INT)									cod_cyp_ent_cbu1,
CAST(cod_suc_cbu1_cr AS INT)							cod_cyp_suc_cr,
CAST(nro_dv_cbu1_cr AS INT)								ds_cyp_nro_dvcbu1,
CAST(nro_fij_cbu2_cr AS INT)							ds_cyp_nro_fijcbu2,
CAST(tpo_cta_cbu2_cr AS INT)							ds_cyp_tpo_ctacbu2,
CAST(cod_mone_cbu2_cr AS INT)							cod_cyp_moneda_cbu2,
CAST(nro_cta_cbu2_cr AS INT)							cod_cyp_nro_ctacbu2,
CAST(nro_dv_cbu2_cr AS INT)								cod_cyp_dv_cbu2,
CAST(nro_arec AS INT)									ds_cyp_nro_arec,
CAST(can_reg AS BIGINT)									ds_cyp_can_reg,
CAST(cant_pgos_rend AS BIGINT)							ds_cyp_can_pgosrend,
CAST(tot_imp_rend AS DECIMAL(15,2))						fc_cyp_tot_imp_rend,
CAST(acum_com_devg AS DECIMAL(15,2))					fc_cyp_acum_comdevg,
CAST(tot_imp_com_cob AS DECIMAL(15,2))					fc_cyp_tot_impcomcob,
CAST(tot_imp_com_devg AS DECIMAL(15,2))					fc_cyp_tot_impcomdvg,
CASE WHEN TRIM(nro_clte)='null'
	 THEN NULL ELSE TRIM(nro_clte) END					ds_cyp_nro_clte,
CASE WHEN TRIM(nom_clte)='null'
	 THEN NULL ELSE TRIM(nom_clte) END					ds_cyp_nom_clte,
CASE WHEN CAST(nro_cuit_clte AS BIGINT)=0
	 THEN NULL ELSE CAST(nro_cuit_clte AS BIGINT) END 	ds_cyp_cuit,
CASE WHEN TRIM(fec_pgo)='null' and trim(hra_pgo)='null'
	 THEN NULL ELSE
	 CONCAT(SUBSTRING(fec_pgo,1,10), ' ',
	 CONCAT(SUBSTRING(hra_pgo,1,2),':',
	 SUBSTRING(hra_pgo,3,2),':',
	 SUBSTRING(hra_pgo,5,2))) END 						ts_cyp_fecha_pago,
CAST(nro_bco_corrs AS INT)								ds_cyp_bco_corrs,
CAST(can_doc AS INT)									ds_cyp_cant_doc,
CAST(can_doc_pdtes_acr AS INT)							ds_cyp_cant_docpdtes,
CAST(tot_pgo AS DECIMAL(15,2))							fc_cyp_tot_pgo,
CAST(imp_com_empr AS DECIMAL(15,2))						fc_cyp_imp_copempr,
CAST(imp_com_3ra AS DECIMAL(15,2))						fc_cyp_imp_cop3ra,
CAST(imp_cotiz_dl_vend AS DECIMAL(15,2))				fc_cyp_imp_cotizvend,
CAST(imp_cotiz_dl_comp AS DECIMAL(15,2))				fc_cyp_imp_cotizcomp,
CAST(suc_orig AS INT)									cod_cyp_sucursal,
CASE WHEN TRIM(nro_boleta)='null'
	 THEN NULL ELSE TRIM(nro_boleta) END				ds_cyp_nro_boleta,
CASE WHEN TRIM(cod_trn)='null'
	 THEN NULL ELSE TRIM(cod_trn) END					ds_cyp_trn,
CASE WHEN TRIM(motivo_ajuste)='null'
	 THEN NULL ELSE TRIM(motivo_ajuste) END				ds_cyp_motivo_ajuste,
CASE WHEN TRIM(signo_ajuste)='null'
	 THEN NULL ELSE TRIM(signo_ajuste) END				ds_cyp_signo_ajuste,
CAST(imp_ajuste AS DECIMAL(15,2))						fc_cyp_imp_ajuste,
CASE WHEN TRIM(cod_captu_caja)='null'
	 THEN NULL ELSE TRIM(cod_captu_caja) END			cod_cyp_captu_caja,
CAST(nro_suc_orig_canl AS INT)							cod_cyp_sucursal_orig,
CASE WHEN TRIM(cod_canal)='null'
	 THEN NULL ELSE TRIM(cod_canal) END					cod_cyp_canal,
CASE WHEN TRIM(cod_sub_canal)='null'
	 THEN NULL ELSE TRIM(cod_sub_canal) END				cod_cyp_subcanal,
CASE WHEN TRIM(des_form_pgo)='null'
	 THEN NULL ELSE TRIM(des_form_pgo) END				ds_cyp_formapago,
CASE WHEN TRIM(nro_instrumento)='null'
	 THEN NULL ELSE TRIM(nro_instrumento) END			ds_cyp_nro_instrumento,
CAST(imp AS DECIMAL(15,2))								fc_cyp_imp,
CASE WHEN TRIM(fec_acred)='null'
	 THEN NULL ELSE SUBSTRING(fec_acred,1,10) END 		dt_cyp_fec_acred,
CASE WHEN TRIM(mar_acreditacion)='null'
	 THEN NULL ELSE TRIM(mar_acreditacion) END 			ds_cyp_mar_acreditacion,
CASE WHEN TRIM(fec_vto_cpd)='null'
	 THEN NULL ELSE SUBSTRING(fec_vto_cpd,1,10) END 	dt_cyp_fec_vtocpd,
CAST(cod_form_pgo AS INT)								cod_cyp_forma_pgo,
CASE WHEN TRIM(cod_mot_rechazo)='null'
	 THEN NULL ELSE TRIM(cod_mot_rechazo) END 			cod_cyp_mot_rechazo,
CASE WHEN TRIM(cod_mot_rend)='null'
	 THEN NULL ELSE TRIM(cod_mot_rend) END 				cod_cyp_mot_rend,
CAST(nro_sec_movim AS INT)								ds_cyp_sec_movim,
CAST(can_otr_impre AS INT)								ds_cyp_can_otrimpre,
CASE WHEN TRIM(tpo_cprb)='null'
	 THEN NULL ELSE TRIM(tpo_cprb) END 					ds_cyp_tpo_cprb,
CASE WHEN TRIM(nro_cprb)='null'
	 THEN NULL ELSE TRIM(nro_cprb) END 					ds_cyp_nro_cprb,
CASE WHEN TRIM(nro_cuo)='null'
	 THEN NULL ELSE TRIM(nro_cuo) END 					ds_cyp_nro_cuo,
CASE WHEN TRIM(fec_vto)='null'
	 THEN NULL ELSE SUBSTRING(fec_vto,1,10) END 		dt_cyp_fecha_vto,
CAST(tsa_puni AS DECIMAL(5,2))							ds_cyp_tsa_puni,
CAST(imp_deuda AS DECIMAL(15,2))						fc_cyp_imp_deuda,
CAST(imp_puni AS DECIMAL(15,2))							fc_cyp_imp_puni,
CAST(imp_dto AS DECIMAL(15,2))							fc_cyp_imp_dto,
CAST(imp_pgo AS DECIMAL(15,2))							fc_cyp_imp_pgo,
CASE WHEN TRIM(obs_libr_1er)='null'
	 THEN NULL ELSE TRIM(obs_libr_1er) END 				ds_cyp_obs_libre1,
CASE WHEN TRIM(obs_libr_2da)='null'
	 THEN NULL ELSE TRIM(obs_libr_2da) END 				ds_cyp_obs_libre2,
CASE WHEN TRIM(obs_libr_3ra)='null'
	 THEN NULL ELSE TRIM(obs_libr_3ra) END 				ds_cyp_obs_libre3,
partition_date
from  bi_corp_staging.rio143_big_arce_rend_cobro_bas
where partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='working_day', dag_id='LOAD_CMN_COBROSYPAGOS') }}'
and '{{ ti.xcom_pull(task_ids='InputConfig', key='working_day', dag_id='LOAD_CMN_COBROSYPAGOS') }}'<'2021-02-01'


UNION ALL

select
CAST(nro_organismo AS BIGINT)							cod_cyp_nro_organismo,
CAST(nro_rend AS INT)									cod_cyp_nro_rend,
CASE WHEN TRIM(tpo_reg)='null'
	 THEN NULL ELSE TRIM(tpo_reg) END					ds_cyp_tpo_reg,
CASE WHEN TRIM(ide_reg)='null'
	 THEN NULL ELSE TRIM(ide_reg) END 					ds_cyp_ide_resgistro,
CAST(sec_ide_reg AS INT)								ds_cyp_sec_registo,
CASE WHEN TRIM(fec_rend)='null'
	 THEN NULL ELSE SUBSTRING(fec_rend,1,10) END 		dt_cyp_fecha,
CASE WHEN TRIM(raz_soc)='null'
	 THEN NULL ELSE TRIM(raz_soc) END					ds_cyp_raz_soc,
CAST(cta_recaud AS INT)									cod_cyp_cta_recau,
CAST(cod_mone_cr AS INT)								cod_cyp_moneda,
CAST(cbu1_cr AS INT)									cod_cyp_ent_cbu1,
CAST(cod_suc_cbu1_cr AS INT)							cod_cyp_suc_cr,
CAST(nro_dv_cbu1_cr AS INT)								ds_cyp_nro_dvcbu1,
CAST(nro_fij_cbu2_cr AS INT)							ds_cyp_nro_fijcbu2,
CAST(tpo_cta_cbu2_cr AS INT)							ds_cyp_tpo_ctacbu2,
CAST(cod_mone_cbu2_cr AS INT)							cod_cyp_moneda_cbu2,
CAST(nro_cta_cbu2_cr AS INT)							cod_cyp_nro_ctacbu2,
CAST(nro_dv_cbu2_cr AS INT)								cod_cyp_dv_cbu2,
CAST(nro_arec AS INT)									ds_cyp_nro_arec,
CAST(can_reg AS BIGINT)									ds_cyp_can_reg,
CAST(cant_pgos_rend AS BIGINT)							ds_cyp_can_pgosrend,
CAST(tot_imp_rend AS DECIMAL(15,2))						fc_cyp_tot_imp_rend,
CAST(acum_com_devg AS DECIMAL(15,2))					fc_cyp_acum_comdevg,
CAST(tot_imp_com_cob AS DECIMAL(15,2))					fc_cyp_tot_impcomcob,
CAST(tot_imp_com_devg AS DECIMAL(15,2))					fc_cyp_tot_impcomdvg,
CASE WHEN TRIM(nro_clte)='null'
	 THEN NULL ELSE TRIM(nro_clte) END					ds_cyp_nro_clte,
CASE WHEN TRIM(nom_clte)='null'
	 THEN NULL ELSE TRIM(nom_clte) END					ds_cyp_nom_clte,
CASE WHEN CAST(nro_cuit_clte AS BIGINT)=0
	 THEN NULL ELSE CAST(nro_cuit_clte AS BIGINT) END 	ds_cyp_cuit,
CASE WHEN TRIM(fec_pgo)='null' and trim(hra_pgo)='null'
	 THEN NULL ELSE
	 CONCAT(SUBSTRING(fec_pgo,1,10), ' ',
	 CONCAT(SUBSTRING(hra_pgo,1,2),':',
	 SUBSTRING(hra_pgo,3,2),':',
	 SUBSTRING(hra_pgo,5,2))) END 						ts_cyp_fecha_pago,
CAST(nro_bco_corrs AS INT)								ds_cyp_bco_corrs,
CAST(can_doc AS INT)									ds_cyp_cant_doc,
CAST(can_doc_pdtes_acr AS INT)							ds_cyp_cant_docpdtes,
CAST(tot_pgo AS DECIMAL(15,2))							fc_cyp_tot_pgo,
CAST(imp_com_empr AS DECIMAL(15,2))						fc_cyp_imp_copempr,
CAST(imp_com_3ra AS DECIMAL(15,2))						fc_cyp_imp_cop3ra,
CAST(imp_cotiz_dl_vend AS DECIMAL(15,2))				fc_cyp_imp_cotizvend,
CAST(imp_cotiz_dl_comp AS DECIMAL(15,2))				fc_cyp_imp_cotizcomp,
CAST(suc_orig AS INT)									cod_cyp_sucursal,
CASE WHEN TRIM(nro_boleta)='null'
	 THEN NULL ELSE TRIM(nro_boleta) END				ds_cyp_nro_boleta,
CASE WHEN TRIM(cod_trn)='null'
	 THEN NULL ELSE TRIM(cod_trn) END					ds_cyp_trn,
CASE WHEN TRIM(motivo_ajuste)='null'
	 THEN NULL ELSE TRIM(motivo_ajuste) END				ds_cyp_motivo_ajuste,
CASE WHEN TRIM(signo_ajuste)='null'
	 THEN NULL ELSE TRIM(signo_ajuste) END				ds_cyp_signo_ajuste,
CAST(imp_ajuste AS DECIMAL(15,2))						fc_cyp_imp_ajuste,
CASE WHEN TRIM(cod_captu_caja)='null'
	 THEN NULL ELSE TRIM(cod_captu_caja) END			cod_cyp_captu_caja,
CAST(nro_suc_orig_canl AS INT)							cod_cyp_sucursal_orig,
CASE WHEN TRIM(cod_canal)='null'
	 THEN NULL ELSE TRIM(cod_canal) END					cod_cyp_canal,
CASE WHEN TRIM(cod_sub_canal)='null'
	 THEN NULL ELSE TRIM(cod_sub_canal) END				cod_cyp_subcanal,
CASE WHEN TRIM(des_form_pgo)='null'
	 THEN NULL ELSE TRIM(des_form_pgo) END				ds_cyp_formapago,
CASE WHEN TRIM(nro_instrumento)='null'
	 THEN NULL ELSE TRIM(nro_instrumento) END			ds_cyp_nro_instrumento,
CAST(imp AS DECIMAL(15,2))								fc_cyp_imp,
CASE WHEN TRIM(fec_acred)='null'
	 THEN NULL ELSE SUBSTRING(fec_acred,1,10) END 		dt_cyp_fec_acred,
CASE WHEN TRIM(mar_acreditacion)='null'
	 THEN NULL ELSE TRIM(mar_acreditacion) END 			ds_cyp_mar_acreditacion,
CASE WHEN TRIM(fec_vto_cpd)='null'
	 THEN NULL ELSE SUBSTRING(fec_vto_cpd,1,10) END 	dt_cyp_fec_vtocpd,
CAST(cod_form_pgo AS INT)								cod_cyp_forma_pgo,
CASE WHEN TRIM(cod_mot_rechazo)='null'
	 THEN NULL ELSE TRIM(cod_mot_rechazo) END 			cod_cyp_mot_rechazo,
CASE WHEN TRIM(cod_mot_rend)='null'
	 THEN NULL ELSE TRIM(cod_mot_rend) END 				cod_cyp_mot_rend,
CAST(nro_sec_movim AS INT)								ds_cyp_sec_movim,
CAST(can_otr_impre AS INT)								ds_cyp_can_otrimpre,
CASE WHEN TRIM(tpo_cprb)='null'
	 THEN NULL ELSE TRIM(tpo_cprb) END 					ds_cyp_tpo_cprb,
CASE WHEN TRIM(nro_cprb)='null'
	 THEN NULL ELSE TRIM(nro_cprb) END 					ds_cyp_nro_cprb,
CASE WHEN TRIM(nro_cuo)='null'
	 THEN NULL ELSE TRIM(nro_cuo) END 					ds_cyp_nro_cuo,
CASE WHEN TRIM(fec_vto)='null'
	 THEN NULL ELSE SUBSTRING(fec_vto,1,10) END 		dt_cyp_fecha_vto,
CAST(tsa_puni AS DECIMAL(5,2))							ds_cyp_tsa_puni,
CAST(imp_deuda AS DECIMAL(15,2))						fc_cyp_imp_deuda,
CAST(imp_puni AS DECIMAL(15,2))							fc_cyp_imp_puni,
CAST(imp_dto AS DECIMAL(15,2))							fc_cyp_imp_dto,
CAST(imp_pgo AS DECIMAL(15,2))							fc_cyp_imp_pgo,
CASE WHEN TRIM(obs_libr_1er)='null'
	 THEN NULL ELSE TRIM(obs_libr_1er) END 				ds_cyp_obs_libre1,
CASE WHEN TRIM(obs_libr_2da)='null'
	 THEN NULL ELSE TRIM(obs_libr_2da) END 				ds_cyp_obs_libre2,
CASE WHEN TRIM(obs_libr_3ra)='null'
	 THEN NULL ELSE TRIM(obs_libr_3ra) END 				ds_cyp_obs_libre3,
partition_date
from  bi_corp_staging.rio143_big_arce_rend_cobro_sice
where partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='working_day', dag_id='LOAD_CMN_COBROSYPAGOS') }}'
and '{{ ti.xcom_pull(task_ids='InputConfig', key='working_day', dag_id='LOAD_CMN_COBROSYPAGOS') }}'<'2021-02-01';