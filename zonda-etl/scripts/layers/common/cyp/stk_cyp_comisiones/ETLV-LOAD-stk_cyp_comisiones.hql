set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
SET mapred.job.queue.name=root.dataeng;


INSERT OVERWRITE TABLE bi_corp_common.stk_cyp_comisiones
PARTITION(partition_date)

select
waprp007_cod_tab_pre     			 				cod_cyp_tab_pre,
waprp007_cod_prod         							cod_cyp_prod,
waprp007_des_tab_pre   								ds_cyp_tab_pre,
waprp007_imp_com_doc      							fc_cyp_imp_comdoc,
waprp007_imp_com_min_doc  							fc_cyp_imp_commindoc,
waprp007_imp_com_max_doc  							fc_cyp_imp_commaxdoc,
waprp007_com_trn          							fc_cyp_com_trn,
waprp007_com_min_trn      							fc_cyp_com_mintrn,
waprp007_com_max_trn      							fc_cyp_com_maxtrn,
waprp007_porm_com         							fc_cyp_porc_com,
waprp007_imp_blta_rec_bas 							fc_cyp_imp_bltarecbas,
waprp007_porm_uso_canl_doc							fc_cyp_porc_usocanldoc,
waprp007_imp_uso_canl_doc 							fc_cyp_imo_usocanldoc,
waprp007_porm_uso_adic    							fc_cyp_porc_usoadic,
waprp007_imp_com_uso_adic 							fc_cyp_imp_comusoadic,
waprp007_porm_uso_canl_trn							fc_cyp_porc_canltrn,
waprp007_imp_uso_canl_trn 							fc_cyp_imp_usocanltrn,
waprp007_imp_com_no_finan 							fc_cyp_imp_comnofinan,
waprp007_imp_com_doc_3ra  							fc_cyp_imp_comdoc3ra,
waprp007_imp_min_doc_3ra  							fc_cyp_imp_mindoc3ra,
waprp007_imp_max_doc_3ra  							fc_cyp_imp_maxdoc3ra,
waprp007_imp_com_trn_3ra  							fc_cyp_com_trn3ra,
waprp007_imp_min_trn_3ra  							fc_cyp_mintrn3ra,
waprp007_imp_max_trn_3ra  							fc_cyp_maxtrn3ra,
waprp007_porm_3ra         							fc_cyp_porc_3ra,
waprp007_imp_recar_no_clte							fc_cyp_imp_recarnoclte,
waprp007_porm_recar_no_clte							fc_cyp_porc_recarnoclte,
waprp007_porm_canl_doc_3ra							fc_cyp_porc_canldoc3ra,
waprp007_imp_canl_doc_3ra 							fc_cyp_imp_canldoc3ra,
waprp007_porm_uso_adic_3ra							fc_cyp_porc_usoadic3ra,
waprp007_imp_uso_adic_3ra 							fc_cyp_imp_usoadic3ra,
waprp007_porm_canl_trn_3ra							fc_cyp_porc_canltrn3ra,
waprp007_imp_canl_trn_3ra 							fc_cyp_imp_canl_trn3ra,
waprp007_imp_no_finan_3ra 							fc_cyp_imp_nofinan3ra,
partition_date
from bi_corp_staging.aprp_waprp007
where partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='working_day', dag_id='LOAD_CMN_COBROSYPAGOS') }}';