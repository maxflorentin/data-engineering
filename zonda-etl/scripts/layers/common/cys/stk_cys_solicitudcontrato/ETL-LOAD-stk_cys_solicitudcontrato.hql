set mapred.job.queue.name=root.dataeng;
set hive.exec.dynamic.partition.mode=nonstrict;	
INSERT OVERWRITE TABLE bi_corp_common.stk_cys_solicitudcontrato
SELECT DISTINCT
	CAST(MP.penumper AS BIGINT) cod_per_nup ,
	CAST(SC.cod_centro AS INT) cod_suc_sucursal,
	CAST(SC.num_solicitu AS BIGINT) cod_cys_nrosolicitud , 
	CAST(SC.cen_contrato AS INT) cod_suc_sucursalcuenta , 
	CAST(SC.num_contrato AS BIGINT) cod_nro_cuenta ,
	SC.cod_producto cod_prod_producto, 
	SC.cod_subprodu cod_prod_subproducto , 
	'{{ ti.xcom_pull(task_ids='InputConfig', key='partition_process_date', dag_id='LOAD_CMN_CyS') }}' dt_cys_processdate ,
	TRIM(SC.cod_usualta) cod_cys_usuarioalta ,
	IF(SC.fec_alta = '0001-01-01', NULL, SC.fec_alta) dt_cys_fechalta , 
	SC.partition_date 
FROM bi_corp_staging.acal_aedtrsc SC
LEFT JOIN bi_corp_staging.malpe_pedt008 MP 
	ON MP.pecodofi = SC.cod_centro 
	AND MP.penumcon = SC.num_contrato 
	AND MP.pecodpro = SC.cod_producto 
	AND MP.pecodsub = SC.cod_subprodu 
	AND MP.partition_date = SC.partition_date 
	AND MP.pecalpar = 'TI'
WHERE SC.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_CyS') }}' ;