"
set mapred.job.queue.name=root.dataeng;
set hive.exec.dynamic.partition.mode=nonstrict;

INSERT OVERWRITE TABLE bi_corp_common.bt_bcaaut_respuestas
PARTITION(partition_date) 
SELECT 
	TRIM(tarjeta) cod_cue_tarjeta ,
	CAST(nup AS BIGINT) cod_per_nup ,
	TRIM(mensaje) id_bcaaut_mensaje ,
	TRIM(resp_cliente_novedad) cod_bcaaut_resp_novedad ,
	IF(resp_cliente_maestro = 'null', NULL, TRIM(resp_cliente_maestro)) cod_bcaaut_resp_maestro ,
	TO_DATE(fecha_alta) dt_bcaaut_alta ,
	IF(fecha_baja = 'null', NULL, TO_DATE(fecha_baja)) dt_bcaaut_baja ,
	TRIM(sigla_atm) cod_bcaaut_sigla ,
	from_unixtime(unix_timestamp(fecha_oferta, 'yyyyMMdd'), 'yyyy-MM-dd') dt_bcaaut_oferta ,
	from_unixtime(unix_timestamp(CONCAT(fecha_oferta , hora_oferta), 'yyyyMMddHHmmss'),'yyyy-MM-dd HH:mm:ss') ts_bcaaut_oferta ,
	IF(tipo_telefono = 'null', NULL, TRIM(tipo_telefono)) ds_bcaaut_tipo_telefono ,
	IF(codigo_area = 'null', NULL, TRIM(codigo_area))ds_bcaaut_cod_area ,
	IF(numero_tel = 'null', NULL, TRIM(numero_tel))ds_bcaaut_nro_telefono ,
	TRIM(tip_doc) ds_bcaaut_tipodoc ,
	CAST(num_doc AS BIGINT) ds_bcaaut_numdoc ,
	partition_date 
 FROM bi_corp_staging.rio102_atm_respuesta_mensajes 
 WHERE partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_BCAAUT_Respuestas') }}' ;
"