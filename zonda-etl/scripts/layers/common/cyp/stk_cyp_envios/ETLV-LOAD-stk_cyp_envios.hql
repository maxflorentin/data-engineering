set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
SET mapred.job.queue.name=root.dataeng;


INSERT OVERWRITE TABLE bi_corp_common.stk_cyp_envios
PARTITION(partition_date)

select
waprp032_nro_empr  														cod_cyp_nro_empr,
waprp032_nro_dig_empr     												cod_cyp_nro_digempr,
waprp032_nro_prod         												cod_cyp_nro_prod,
waprp032_nro_instan     												ds_cyp_nro_instan,
waprp032_tpo_envio        												cod_cyp_tpo_envio,
CASE WHEN trim(waprp032_fec_alta_envio)='0001-01-01'
	 THEN NULL ELSE waprp032_fec_alta_envio END 								dt_cyp_fec_altaenvio,
waprp032_nro_envio        						ds_cyp_nro_envio,
waprp032_cod_canal        						cod_cyp_canal,
waprp032_cod_estado       						cod_cyp_estado,
waprp032_can_doc_acept    						ds_cyp_can_docacept,
waprp032_can_doc_rech     						ds_cyp_can_docrech,
waprp032_can_doc_inf      						ds_cyp_can_docinf,
waprp032_can_pgo          						ds_cyp_can_pgo,
waprp032_can_otr_impre    						ds_cyp_cant_otrimpre,
waprp032_can_liq          						ds_cyp_can_liq,
waprp032_imp_acept        						fc_cyp_imp_acept,
waprp032_imp_rech         						fc_cyp_imp_rech,
waprp032_imp_inf          						fc_cyp_imp_inf,
CASE WHEN trim(waprp032_fec_autz)='0001-01-01'
	 THEN NULL ELSE  waprp032_fec_autz END         						dt_cyp_fec_autz,
CASE WHEN trim(waprp032_ide_user_autz)=''
	THEN NULL ELSE waprp032_ide_user_autz END 							cod_cyp_user_autz,
CASE WHEN trim(waprp032_cod_mot_rech)=''
	THEN NULL ELSE waprp032_cod_mot_rech END      						cod_cyp_mot_rech,
CASE WHEN trim(waprp032_fec_baj_envio)='0001-01-01'
	 THEN NULL ELSE  waprp032_fec_baj_envio END    						dt_cyp_fec_bajenvio,
CASE WHEN trim(waprp032_mar_vuelco)=''
	THEN NULL ELSE waprp032_mar_vuelco END       												ds_cyp_mar_vuelco,
CASE WHEN trim(waprp032_mar_soli_impre)='S'
	THEN 1 ELSE 0 END   												flag_cyp_soli_impre,
CASE WHEN trim(waprp032_mar_ctrl_firma)='S'
	THEN 1 ELSE 0 END   												flag_cyp_ctrl_firma,
CASE WHEN trim(waprp032_mar_autz)='S'
	THEN 1 ELSE 0 END         											flag_cyp_mar_autz,
waprp032_nro_suc_orig_acdo												cod_cyp_suc_origacdo,
CASE WHEN trim(waprp032_fec_max_baja)='0001-01-01'
	 THEN NULL ELSE  waprp032_fec_max_baja END     						dt_cyp_fec_maxbaja,
CASE WHEN trim(waprp032_fec_confirm)='0001-01-01'
	 THEN NULL ELSE  waprp032_fec_confirm END      						dt_cyp_fec_confirm,
CASE WHEN trim(waprp032_mar_act_deud)=''
	THEN NULL ELSE waprp032_mar_act_deud END       						ds_cyp_mar_actdeud,
waprp032_nro_ult_rend       											ds_cyp_nro_ultrend,
waprp032_per_envi			      			    						ds_cyp_periodo,
CASE WHEN trim(waprp032_fec_dde)='0001-01-01'
	 THEN NULL ELSE  waprp032_fec_dde END          						dt_cyp_fecha_desde,
CASE WHEN trim(waprp032_fec_hta)='0001-01-01'
	 THEN NULL ELSE  waprp032_fec_hta END          						dt_cyp_fecha_hasta,
waprp032_cod_cpto_env     												cod_cyp_cpto_envio,
waprp065_cod_form_pgo													cod_cyp_form_pgo,
waprp065_cod_mone														cod_cyp_moneda,
waprp065_fec_disp														dt_cyp_fec_disp,
waprp065_ide_cta_cd_cc 													cod_cyp_cta_creddeb,
waprp065_imp															fc_cyp_imp,
waprp065_imp_baja														fc_cyp_imp_baja,
waprp065_fec_emision_calc												dt_cyp_fec_emisioncal,
ap032.partition_date

from  bi_corp_staging.aprp_waprp032 ap032
left join bi_corp_staging.aprp_waprp065 ap065 on
(waprp065_nro_empr=waprp032_nro_empr and
waprp032_nro_envio=waprp065_nro_envio and
waprp032_fec_alta_envio=waprp065_fec_alta_envio and
ap065.partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='working_day', dag_id='LOAD_CMN_COBROSYPAGOS') }}')
where ap032.partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='working_day', dag_id='LOAD_CMN_COBROSYPAGOS') }}';
