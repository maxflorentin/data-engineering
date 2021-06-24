set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
SET mapred.job.queue.name=root.dataeng;


INSERT OVERWRITE TABLE bi_corp_common.stk_cyp_recdeuda
PARTITION(partition_date)

select
warce001_nro_empr      												cod_cyp_nro_empr,
warce001_nro_dig_empr												cod_cyp_nro_digempr,
warce001_nro_prod													cod_cyp_nro_prod,
warce001_nro_instan													ds_cyp_nro_instan,
warce001_nro_clte													cod_cyp_nro_clte,
warce001_tpo_cprb													ds_cyp_tpo_cprb,
warce001_nro_cprb													ds_cyp_nro_cprb,
warce001_nro_cuo													ds_cyp_nro_cuo,
CONCAT(SUBSTRING(warce001_fec_hra_alta_deud,1,10),
' ',TRANSLATE(SUBSTRING(warce001_fec_hra_alta_deud,12,8),'.',':'))	ts_cyp_alta_deud,
warce001_nom_clte													ds_cyp_nom_clte,
CASE WHEN trim(warce001_des_dire)='SIN DIRECCION'
	 THEN NULL ELSE warce001_des_dire END 							ds_cyp_direccion,
CASE WHEN trim(warce001_des_loc)='SIN LOCALIDAD'
	 THEN NULL ELSE warce001_des_loc END													ds_cyp_localidad,
CASE WHEN TRIM(warce001_cod_pref_cpost) =''
	 THEN NULL ELSE TRIM(warce001_cod_pref_cpost)  END 				ds_cyp_codigopostal,
warce001_nro_cpost													ds_cyp_nro_codigopostal,
CASE WHEN TRIM(warce001_cod_ubic_cpost) =''
	 THEN NULL ELSE TRIM(warce001_cod_ubic_cpost) END 				ds_cyp_cod_ubiccpost,
CASE WHEN warce001_nro_cuit_clte= 0
	 THEN NULL ELSE warce001_nro_cuit_clte END 	ds_cyp_cuit_clte,
warce001_ing_brutos													cod_cyp_ing_brutos,
warce001_cod_cond_iva												cod_cyp_cond_iva,
CASE WHEN TRIM(warce001_fec_1er_vto)='9999-01-01'
	THEN NULL ELSE TRIM(warce001_fec_1er_vto) END					dt_cyp_1er_vto,
warce001_imp_1er_vto												fc_cyp_imp_1ervto,
CASE WHEN TRIM(warce001_fec_2do_vto)='0001-01-01'
	THEN NULL ELSE TRIM(warce001_fec_2do_vto) END					dt_cyp_2do_vto,
warce001_imp_2do_vto												fc_cyp_imp_2dovto,
CASE WHEN TRIM(warce001_fec_hasta_dscto)='0001-01-01'
	THEN NULL ELSE TRIM(warce001_fec_hasta_dscto) END				dt_cyp_hasta_dscto,
warce001_imp_pronto_pago											fc_cyp_imp_prontopago,
CASE WHEN TRIM(warce001_fec_hasta_punit)='0001-01-01'
	THEN NULL ELSE TRIM(warce001_fec_hasta_punit) END				dt_cyp_hasta_punit,
warce001_tsa_puni													ds_cyp_tsa_puni,
warce001_mar_excep_3ra												ds_cyp_mar_excep3ra,
CASE WHEN TRIM(warce001_cod_form_pgo) =''
	 THEN NULL ELSE TRIM(warce001_cod_form_pgo) ='' END 			cod_cyp_form_pgo,
warce001_imp_descto													fc_cyp_imp_descto,
warce001_imp_int_deven												fc_cyp_imp_intdeven,
warce001_mon_sdo_act												fc_cyp_mon_sdoact,
CASE WHEN TRIM(warce001_fec_ult_pgo)='0001-01-01'
	THEN NULL ELSE TRIM(warce001_fec_ult_pgo) END					dt_cyp_ult_pgo,
warce001_tot_sdo_act												fc_cyp_tot_sdoact,
CASE WHEN TRIM(warce001_cod_concept) =''
	 THEN NULL ELSE TRIM(warce001_cod_concept)  END 				cod_cyp_concep,
CASE WHEN TRIM(warce001_des_doc) =''
	 THEN NULL ELSE TRIM(warce001_des_doc) END 		    			ds_cyp_doc,
CASE WHEN TRIM(warce001_obs_libr_1er) =''
	 THEN NULL ELSE TRIM(warce001_obs_libr_1er) END 		    	ds_cyp_obs1,
CASE WHEN TRIM(warce001_obs_libr_2da) =''
	 THEN NULL ELSE TRIM(warce001_obs_libr_2da) END 		    	ds_cyp_obs2,
CASE WHEN TRIM(warce001_obs_libr_3ra) =''
	 THEN NULL ELSE TRIM(warce001_obs_libr_3ra) END 		    	ds_cyp_obs3,
CASE WHEN TRIM(warce001_obs_libr_4ta) =''
	 THEN NULL ELSE TRIM(warce001_obs_libr_4ta) END 		    	ds_cyp_obs4,
CASE WHEN TRIM(warce001_obs_libr_5ta) =''
	 THEN NULL ELSE TRIM(warce001_obs_libr_5ta) END 		    	ds_cyp_obs5,
warce001_nro_prox_rend												ds_cyp_nro_proxrend,
warce001_tpo_ingreso												cod_cyp_tpo_ingreso,
warce001_fec_alta													dt_cyp_alta,
warce001_nro_envio													ds_cyp_nro_envio,
warce001_cod_mone													cod_cyp_moneda,
CASE WHEN TRIM(warce001_cod_canal_fpago) =''
	 THEN NULL ELSE TRIM(warce001_cod_canal_fpago) END 		    	cod_cyp_canal_fpago,
partition_date														partition_date

from  bi_corp_staging.arce_warce001
where partition_date ='{{ ti.xcom_pull(task_ids='InputConfig', key='working_day', dag_id='LOAD_CMN_COBROSYPAGOS') }}';
