set mapred.job.queue.name=root.dataeng;
set hive.exec.dynamic.partition.mode=nonstrict;
-----------------------------------------------
INSERT OVERWRITE TABLE bi_corp_common.stk_bcaaut_remanentes
PARTITION(partition_date)  
SELECT 
   SUBSTRING(RM.fecha, 1, 19) ts_bcaaut_fecha
 , RM.terminal cod_bcaaut_sigla
 , TM.cod_bcaaut_sucursal cod_bcaaut_sucursal
 , TM.ds_bcaaut_sucursal ds_bcaaut_sucursal
 , TM.cod_bcaaut_zona
 , TM.ds_bcaaut_operador
 , TM.ds_bcaaut_posiciontipo 
 , TM.ds_bcaaut_tipoequipo ds_bcaaut_tipoequipo
 , NULL fc_bcaaut_cargado
 , NULL fc_bcaaut_dispensado
 , CAST(RM.remanente AS INT) fc_bcaaut_remanente
 , RM.partition_date 
FROM bi_corp_staging.rio155_atm_remanentes RM
	LEFT JOIN bi_corp_common.dim_bcaaut_terminales TM
		ON TM.cod_bcaaut_sigla = TRIM(RM.terminal)
		AND TM.partition_date = RM.partition_date 
WHERE RM.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_BCAAUT_Terminales') }}';