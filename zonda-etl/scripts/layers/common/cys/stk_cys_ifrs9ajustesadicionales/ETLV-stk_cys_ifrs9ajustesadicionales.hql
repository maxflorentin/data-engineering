set mapred.job.queue.name=root.dataeng;
set hive.exec.dynamic.partition.mode=nonstrict;

INSERT OVERWRITE TABLE bi_corp_common.stk_cys_ifrs9ajustesadicionales
PARTITION(partition_date) 
SELECT dt_periodo dt_cys_periodo 
	, UPPER(TRIM(ds_portfolio_ifrs9)) ds_cys_portfolioifrs9
	, UPPER(TRIM(ds_segmento_control)) ds_cys_segmentocontrol
	, fc_draw_balance fc_cys_drawnbalance
	, fc_undraw_balance fc_cys_undrawnbalance
	, fc_ead fc_cys_ead
	, fc_provision_in_balance_amount fc_cys_provisioninbalanceamount
	, fc_provision_out_balance_amount fc_cys_provisionoutbalanceamount
	, fc_provision	fc_cys_provision
	, int_stagefinal fc_cys_stagefinal
	, fc_rof fc_cys_rof
	, fc_insolvencias_rof fc_cys_insolvenciasrof
	, fc_nuevo_rof fc_cys_nuevorof 
	, IF(TRIM(ds_origen) = '""', NULL, TRIM(ds_origen)) ds_cys_origen
	, TRIM(ds_rubro_cargabal_in_provision) cod_cys_rubrocargabalinprovision
	, TRIM(ds_rubro_cargabal_out_provision) cod_cys_rubrocargabaloutprovision
	, '{{ ti.xcom_pull(task_ids='InputConfig', key='previous_month_last_working_day', dag_id='LOAD_CMN_CyS-Ajustes_Cartera') }}' partition_date
FROM bi_corp_risk.ifrs_ajustes_adicionales 
WHERE dt_periodo = '{{ ti.xcom_pull(task_ids='InputConfig', key='previous_month', dag_id='LOAD_CMN_CyS-Ajustes_Cartera') }}';
