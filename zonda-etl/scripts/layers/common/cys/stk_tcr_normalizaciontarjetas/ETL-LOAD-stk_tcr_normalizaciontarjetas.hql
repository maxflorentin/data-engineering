set mapred.job.queue.name=root.dataeng;
set hive.exec.dynamic.partition.mode=nonstrict;
-----------------------------------------------
INSERT OVERWRITE TABLE bi_corp_common.stk_tcr_normalizaciontarjetas
PARTITION(partition_date)
SELECT DISTINCT CAST(gitccuo_num_persona AS BIGINT) cod_per_nup 	 
	, TRIM(gitccuo_cod_entigen) cod_tcr_entidad 	
	, CAST(gitccuo_cod_centro AS INT) cod_suc_sucursal 	 
	, CAST(gitccuo_num_contrato AS BIGINT) cod_tcr_cuenta 	
	, TRIM(gitccuo_cod_producto) cod_tcr_producto 	
	, TRIM(gitccuo_cod_subprodu) cod_tcr_subproducto 	
	, TRIM(gitccuo_cod_divisa) cod_tcr_divisa 	
	, CAST(gitccuo_num_secuencia AS INT)  int_tcr_secuencia 	 
	, TRIM(gitccuo_fec_cuotaphone) dt_tcr_cuotaphone 	 
	, CAST(gitccuo_cuenta_visa AS BIGINT) cod_tcr_cuentabase 	
	, TRIM(gitccuo_cod_marca_ini) cod_tcr_marcainicial 	
	, TRIM(gitccuo_cod_submarca_ini) cod_tcr_submarcainicial 	
	, TRIM(gitccuo_cod_marca_seg_ini) cod_tcr_marcasegini 	
	, TRIM(gitccuo_tip_reestruct_ini) cod_tcr_tiporeestructuracionini 	
	, TRIM(gitccuo_cod_marca_act) cod_tcr_marcaactual 	
	, TRIM(gitccuo_cod_submarca_act) cod_tcr_submarcaactual 	
	, TRIM(gitccuo_fec_cambio_seg) dt_tcr_cambioseg 	 
	, TRIM(gitccuo_cod_marca_seg_act) cod_tcr_marcasegact 	 
	, TRIM(gitccuo_tip_reestruct_act) cod_tcr_tiporeestructuracionact 	
	, TRIM(gitccuo_fec_cura) dt_tcr_cura 	 
	, IF(TRIM(gitccuo_fec_empeora) = '9999-12-31', NULL, TRIM(gitccuo_fec_empeora)) dt_tcr_empeora 	 
	, IF(TRIM(gitccuo_fec_normaliza) = '9999-12-31', NULL, TRIM(gitccuo_fec_normaliza)) dt_tcr_normaliza 	 
	, TRIM(gitccuo_motivo_cambio) ds_tcr_motivocambio 	
	, CAST(gitccuo_num_ree AS INT) int_tcr_ree 	 
	, SUBSTRING(gitccuo_timest_umo, 1, 10) dt_tcr_carga 	 
	, partition_date 	
FROM bi_corp_staging.garra_log_cuotaphone 
WHERE partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_CyS') }}' ;