set mapred.job.queue.name=root.dataeng;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;


INSERT OVERWRITE TABLE bi_corp_common.stk_cyp_documentospagados
PARTITION(partition_date)

select
warce005_ide_pgo   								cod_cyp_ide_pgo,
warce005_tpo_cprb         						ds_cyp_tpo_cprb,
warce005_nro_cprb         						ds_cyp_nro_cprb,
warce005_nro_cuo          						ds_cyp_nro_cuo,
warce005_nro_empr         						cod_cyp_nro_empr,
warce005_nro_dig_empr     						cod_cyp_nro_digempr,
warce005_nro_prod         						cod_cyp_nro_prod,
warce005_nro_instan       						ds_cyp_nro_instan,
CASE WHEN trim(warce005_nro_clte)=''
	 THEN NULL ELSE warce005_nro_clte END 		cod_cyp_nro_clte,
CASE WHEN warce005_fec_vto='9999-01-01'
	 THEN NULL ELSE warce005_fec_vto END 		dt_cyp_fec_vto ,
warce005_sdo_ant          						fc_cyp_sldo_ant,
CASE WHEN warce005_fec_pgo_ant='0001-01-01'
	 THEN NULL ELSE warce005_fec_pgo_ant END    dt_cyp_fec_pgoant,
warce005_tsa_punitorios   						ds_cyp_tsa_punitorios,
warce005_imp_dif_vto      						fc_cyp_imp_difvto,
warce005_imp_puni         						fc_cyp_imp_puni,
warce005_imp_dto          						fc_cyp_imp_dto,
warce005_imp_pgo          						fc_cyp_imp_pgo,
CASE WHEN trim(warce005_obs_libr_1er)=''
	 THEN NULL ELSE warce005_obs_libr_1er END      						ds_cyp_obs1,
CASE WHEN trim(warce005_obs_libr_2da)=''
	 THEN NULL ELSE warce005_obs_libr_2da END      						ds_cyp_obs2,
CASE WHEN trim(warce005_obs_libr_3ra)=''
	 THEN NULL ELSE warce005_obs_libr_3ra END      						ds_cyp_obs3,
warce005_cod_mone         						cod_cyp_mone,
partition_date
from  bi_corp_staging.arce_warce005
where partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='working_day', dag_id='LOAD_CMN_COBROSYPAGOS') }}';