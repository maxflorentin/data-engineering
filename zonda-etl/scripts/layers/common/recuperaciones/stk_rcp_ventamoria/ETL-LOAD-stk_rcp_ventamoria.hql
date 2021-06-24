SET hive.merge.mapfiles=true;
SET hive.merge.mapredfiles=true;
SET hive.merge.size.per.task=4000000;
SET hive.merge.smallfiles.avgsize=16000000;
SET hive.exec.dynamic.partition.mode=nonstrict;
SET mapred.job.queue.name=root.dataeng;

INSERT OVERWRITE TABLE bi_corp_common.stk_rcp_ventamoria 
PARTITION(partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_RECUPERACIONES-Daily') }}')

SELECT 
	CAST(idventa AS INT) idventa ,
	CAST(idfideicomiso AS INT) idfideicomiso ,
	CAST(idpreventa AS INT) idpreventa ,
	CAST(cotizacion AS INT) cotizacion ,
	TRIM(fecha_corte) fecha_corte ,
	IF(TRIM(ip) = '', NULL, TRIM(ip)) ip ,
	IF(TRIM(usuario) = '', NULL, TRIM(usuario)) usuario ,
	TRIM(estado) estado ,
	TRIM(fecha_preventa) fecha_preventa ,
	TRIM(clientes_agregados) clientes_agregados ,
	TRIM(clientes_excluidos) clientes_excluidos ,
	TRIM(fecha_venta) fecha_venta ,
	TRIM(usuario_alta) usuario_alta ,
	from_unixtime(unix_timestamp(timest_alta ,'dd-MM-yyyy HH:mm:ss'),'yyyy-MM-dd  HH:mm:ss') timest_alta , 
	TRIM(usuario_envio) usuario_envio ,
	from_unixtime(unix_timestamp(timest_envio ,'dd-MM-yyyy HH:mm:ss'),'yyyy-MM-dd  HH:mm:ss') timest_envio , 
	TRIM(usuario_aprobacion) usuario_aprobacion ,
	from_unixtime(unix_timestamp(timest_aprobacion ,'dd-MM-yyyy HH:mm:ss'),'yyyy-MM-dd  HH:mm:ss') timest_aprobacion , 
	IF(TRIM(motivo_rechazo) = '', NULL, TRIM(motivo_rechazo)) motivo_rechazo ,
	IF(TRIM(fec_ajuste1) = '', NULL, TRIM(fec_ajuste1)) fec_ajuste1 ,
	IF(TRIM(fec_ajuste2) = '', NULL, TRIM(fec_ajuste2)) fec_ajuste2 ,
	partition_date
FROM bi_corp_staging.moria_vc_ventas 
WHERE partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_RECUPERACIONES-Daily') }}' ;