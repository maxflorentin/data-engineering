set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
SET mapred.job.queue.name=root.dataeng;


INSERT OVERWRITE TABLE bi_corp_common.stk_cyp_acuerdos
PARTITION(partition_date)

select
waprp055_nro_empr   									cod_cyp_nro_empr,
waprp055_nro_dig_empr      								cod_cyp_nro_digempr,
waprp055_nro_prod         								cod_cyp_nro_prod,
waprp055_nro_instan       								ds_cyp_nro_instan,
waprp055_des_acdo          								ds_cyp_des_acdo,
waprp055_nro_ddn_tel       								ds_cyp_nro_ddntel,
waprp055_nro_carac_tel     								ds_cyp_nro_caracttel,
waprp055_nro_tel           								ds_cyp_nro_tel,
waprp055_cod_mone          								cod_cyp_mone,
waprp055_nro_ent_orig_acdo 								ds_cyp_nro_origacdo,
waprp055_nro_suc_orig_acdo 								ds_cyp_suc_origacdo,
CASE WHEN TRIM(waprp055_mar_com)='S'
	 THEN 1 ELSE 0 END 									flag_cyp_com,
CASE WHEN TRIM(waprp055_mar_cobr_com)='S'
	 THEN 1 ELSE 0 END 		      						flag_cyp_cobr_com,
waprp055_nro_ult_rend      								ds_cyp_nro_ultrend,
CASE WHEN TRIM(waprp055_mar_inf_datanet)='S'
	 THEN 1 ELSE 0 END 		         					flag_cyp_datanet,
CASE WHEN TRIM(waprp055_mar_deuda)='S'
	 THEN 1 ELSE 0 END 		            				flag_cyp_deuda,
CASE WHEN TRIM(waprp055_mar_form_pgo)='S'
	 THEN 1 ELSE 0 END 		    						flag_cyp_form_pgo,
CASE WHEN TRIM(waprp055_mar_rend)='S'
	 THEN 1 ELSE 0 END 		    						flag_cyp_rend,
CASE WHEN TRIM(waprp055_mar_recau)='S'
	 THEN 1 ELSE 0 END 		 							flag_cyp_recau,
CASE WHEN TRIM(waprp055_mar_pago)='S'
	 THEN 1 ELSE 0 END 		   							flag_cyp_pago,
waprp055_cod_plan_contb    								cod_cyp_plan_contb,
CASE WHEN waprp055_fec_ult_recep_deud='0001-01-01'
	 THEN NULL ELSE waprp055_fec_ult_recep_deud END 	dt_cyp_ult_recepdeud,
waprp055_estado            								cod_cyp_estado,
CASE WHEN waprp055_fec_alt_acdo='0001-01-01'
	 THEN NULL ELSE waprp055_fec_alt_acdo END       	dt_cyp_fec_altacdo,
CASE WHEN waprp055_fec_baj_acdo='0001-01-01'
	 THEN NULL ELSE waprp055_fec_baj_acdo END       	dt_cyp_fec_bajacdo,
waprp055_cod_mot_baj_acdo 								cod_cyp_mot_bajacdo,
waprp055_mar_confirming    								ds_cyp_mar_confirming,
CASE WHEN TRIM(waprp055_mar_factoring)='S'
	 THEN 1 ELSE 0 END									flag_cyp_factoring,
CASE WHEN TRIM(waprp055_mar_float)=''
	 THEN NULL ELSE waprp055_mar_float END				ds_cyp_mar_float,
CASE WHEN TRIM(waprp055_mar_ctrl_firma)='S'
	 THEN 1 ELSE 0 END 		 							flag_cyp_ctrl_firma,
CASE WHEN TRIM(waprp055_mar_forz_batch)='S'
	 THEN 1 ELSE 0 END 		 							flag_cyp_forz_batch,
CASE WHEN TRIM(waprp055_mar_inh_soli_impre)='S'
	 THEN 1 ELSE 0 END 		 							flag_cyp_inh_soliimpre,
CASE WHEN TRIM(waprp055_mar_ctrl_sdo)='S'
	 THEN 1 ELSE 0 END 		 							flag_cyp_ctrl_sdo,
CASE WHEN TRIM(waprp055_mar_uso)='S'
	 THEN 1 ELSE 0 END 		 							flag_cyp_uso,
CASE WHEN TRIM(waprp055_mar_aut_automat)='S'
	 THEN 1 ELSE 0 END 		 							flag_cyp_autz_auto,
CASE WHEN TRIM(waprp055_mar_entr_canal)='S'
	 THEN 1 ELSE 0 END 		  							flag_cyp_entrcanal,
CASE WHEN TRIM(waprp055_mar_tipo_dist)='S'
	 THEN 1 ELSE 0 END 		  							flag_cyp_mar_tipodist,
waspe050_nroemprarec									cod_cyp_nroempre_migrada,
CASE WHEN waspe050_fecmigracion='0001-01-01'
	 THEN NULL ELSE waspe050_fecmigracion END 			dt_cyp_fec_migracion,
waprp038_ide_tab_carac									cod_cyp_tab_carac,
waprp038_nro_lista										ds_cyp_nro_lista,
waprp038_nro_nue_lista									ds_cyo_nro_nuelista,
CASE WHEN waprp038_fec_vigencia='0001-01-01'
	 THEN NULL ELSE waprp038_fec_vigencia END 			dt_cyp_fec_vigencia,
waprp062_des_tab_form_pgo								ds_cyp_form_pgo,
waprp040_des_carac_com									ds_cyp_caract_com,
waprp040_tpo_com										ds_cyp_tpo_com,
waprp040_cod_per_cobr_com								ds_cyp_periodo_cobrcom,
CASE WHEN TRIM(waprp040_mar_disc_com)='S'
	 THEN 1 ELSE 0 END 		  							flag_cyp_discrimina_com,
waprp040_tpo_debito										cod_cyo_tpo_debito,
waprp039_des_caract_rec									ds_cyp_caract_rec,
CASE WHEN TRIM(waprp039_mar_pgo_parc)='S'
	 THEN 1 ELSE 0 END 		  							flag_cyp_pgo_parc,
waprp039_mar_acr_lin									cod_cyp_mar_acrlin,
CASE WHEN TRIM(waprp039_mar_bco_corrs)='S'
	 THEN 1 ELSE 0 END 		  							flag_cyp_bco_corrs,
CASE WHEN TRIM(waprp039_cod_per_pgo)=''
	 THEN NULL ELSE waprp039_cod_per_pgo END 		  	ds_cyp_periodo_pgo,
waprp039_mar_cpd_cust									ds_cyp_mar_cpdcust,
waprp039_nro_lim_vto_cpd								ds_cyp_nro_limvtocpd,
CASE WHEN TRIM(waprp039_mar_redep_cheq)='S'
	 THEN 1 ELSE 0 END 		  							flag_cyp_redep_cheq,
CASE WHEN TRIM(waprp039_mar_pgo_integ)='S'
	 THEN 1 ELSE 0 END 		  							flag_cyp_pgo_integ,
waprp039_porc_acept_dnf									fc_cyp_porc_aceptdnf,
CASE WHEN TRIM(waprp039_mar_echeq_acep)='S'
	 THEN 1 ELSE 0 END 		  							flag_cyp_echeq_acep,
CASE WHEN TRIM(waprp039_mar_echeq_terceros)='S'
	 THEN 1 ELSE 0 END 		  							flag_cyp_echeq_terceros,
CASE WHEN TRIM(waprp039_mar_echeq_clte)='S'
	 THEN 1 ELSE 0 END 		  							flag_cyp_echeq_clte,
CASE WHEN TRIM(waprp039_mar_echeq_factura)='S'
	 THEN 1 ELSE 0 END 		  							flag_cyp_echeq_factura,
waprp039_frec_int										ds_cyp_frec_int,
waprp043_des_carac_rend									ds_cyp_caract_rend,
CASE WHEN TRIM(waprp043_cod_med_rend)=''
	 THEN NULL ELSE waprp043_cod_med_rend END			cod_cyp_med_rend,
CASE WHEN TRIM(waprp043_mar_lst_rend)='S'
	 THEN 1 ELSE 0 END 		  							flag_cyp_lista_rend,
CASE WHEN TRIM(waprp043_cod_envi_rend_sin)='S'
	 THEN 1 ELSE 0 END 		  							flag_cyp_envio_rend,
waprp043_cod_per_rend									ds_cyp_periodo_rend,
CASE WHEN TRIM(waprp043_cod_dest_rend)=''
	 THEN NULL ELSE waprp043_cod_med_rend END			cod_cyp_dest_rend,
CASE WHEN TRIM(waprp043_mar_rend_ajuste)='S'
	 THEN 1 ELSE 0 END 		  							flag_cyp_ajuste_rend,
CASE WHEN TRIM(waprp043_cod_inf_datanet)=''
	 THEN NULL ELSE waprp043_cod_inf_datanet END		cod_cyp_info_datanet,
waprp042_des_caract_deuda								ds_cyp_caract_deuda,
waprp042_cod_cuo										cod_cyp_cuo,
waprp042_tpo_vto										ds_cyp_tpo_vto,
CASE WHEN TRIM(waprp042_mar_lim_cheq)=''
	 THEN NULL ELSE waprp043_cod_inf_datanet END		ds_cyp_lim_cheq,
CASE WHEN TRIM(waprp042_mar_dto)='S'
	 THEN 1 ELSE 0 END 		  							flag_cyp_dto,
CASE WHEN TRIM(waprp042_mar_puni)='S'
	 THEN 1 ELSE 0 END 		  							flag_cyp_puni,
waprp042_can_dia_lat									ds_cyp_dia_lat,
CASE WHEN TRIM(waprp042_mar_carga_manual)='S'
	 THEN 1 ELSE 0 END 		  							flag_cyp_carga_manual,
CASE WHEN TRIM(waprp042_mar_fact_elect)='S'
	 THEN 1 ELSE 0 END 		  							flag_cyp_fact_elect,
CASE WHEN TRIM(waprp042_publica_debin)='S'
	 THEN 1 ELSE 0 END 		  							flag_cyp_publica_debin,
CASE WHEN TRIM(waprp042_mar_gen_cvu)='S'
	 THEN 1 ELSE 0 END 		  							flag_cyp_gen_cvu,
waprp053_des_carac_pago									ds_cyp_carac_pago,
CASE WHEN TRIM(waprp053_mar_cntrl_secu)='S'
	 THEN 1 ELSE 0 END 		  							flag_cyp_cntrl_secu,
CASE WHEN TRIM(waprp053_mar_autz)='S'
	 THEN 1 ELSE 0 END 		  							flag_cyp_autz,
waprp053_nro_empr_distr									ds_cyp_nro_emprdistr,
waprp053_cod_trat_fec									cod_cyp_fec_efec,
CASE WHEN TRIM(waprp053_mar_exenta_iibb)='S'
	 THEN 1 ELSE 0 END 		  							flag_cyp_exenta_iibb,
ap055.partition_date

from bi_corp_staging.aprp_waprp055 ap055
left join bi_corp_staging.aprp_waprp038 ap038 on
(waprp055_nro_empr=waprp038_nro_empr
and waprp038_nro_dig_empr=waprp055_nro_dig_empr
and waprp038_nro_prod=waprp055_nro_prod
and waprp038_nro_instan=waprp055_nro_instan
and ap038.partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='working_day', dag_id='LOAD_CMN_COBROSYPAGOS') }}')
left join bi_corp_staging.aspe_waspe050 as050 on (waprp055_nro_empr=waspe050_nroempr and as050.partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='working_day', dag_id='LOAD_CMN_COBROSYPAGOS') }}')
left join bi_corp_staging.aprp_waprp062 ap062 on (waprp038_ide_tab_carac=waprp062_cod_tab_form_pgo and ap062.partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='working_day', dag_id='LOAD_CMN_COBROSYPAGOS') }}')
left join bi_corp_staging.aprp_waprp040 ap040 on
(waprp040_nro_lista=waprp038_nro_lista
and waprp038_nro_prod= waprp040_nro_prod
and ap040.partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='working_day', dag_id='LOAD_CMN_COBROSYPAGOS') }}')
left join bi_corp_staging.aprp_waprp039 ap039 on
(waprp039_nro_lista=waprp038_nro_lista
and waprp038_nro_prod= waprp039_nro_prod
and ap039.partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='working_day', dag_id='LOAD_CMN_COBROSYPAGOS') }}')
left join bi_corp_staging.aprp_waprp043 ap043 on
(waprp043_nro_lista=waprp038_nro_lista
and waprp038_nro_prod= waprp043_nro_prod
and ap043.partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='working_day', dag_id='LOAD_CMN_COBROSYPAGOS') }}')
left join bi_corp_staging.aprp_waprp042 ap042 on
(waprp042_nro_lista=waprp038_nro_lista
and waprp038_nro_prod= waprp042_nro_prod
and ap042.partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='working_day', dag_id='LOAD_CMN_COBROSYPAGOS') }}')
left join bi_corp_staging.aprp_waprp053 ap053 on
(waprp053_nro_lista=waprp038_nro_lista
and waprp038_nro_prod= waprp053_nro_prod
and ap053.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='working_day', dag_id='LOAD_CMN_COBROSYPAGOS') }}')

where ap055.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='working_day', dag_id='LOAD_CMN_COBROSYPAGOS') }}';
