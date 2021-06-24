"
set mapred.job.queue.name=root.dataeng;
set hive.exec.dynamic.partition.mode=nonstrict;

INSERT
	OVERWRITE TABLE bi_corp_common.dim_rec_centro_costo
SELECT
	TRIM(cco.cod_sucursal) cod_suc_sucursal ,          
	TRIM(cco.cod_oficina) cod_rec_oficia ,            
	TRIM(cco.descripcion) ds_rec_centro_costo,           
	TRIM(cco.id_actor) cod_rec_actor ,          
	TRIM(cco.id_usuario_alta) cod_rec_usuario_alta ,            
	from_unixtime(unix_timestamp(cco.fecha_alta ,'dd/MM/yyyy HH:mm:ss'))  dt_rec_alta ,            
	TRIM(cco.id_usuario_modif) ,            
	from_unixtime(unix_timestamp(cco.fecha_modif ,'dd/MM/yyyy HH:mm:ss')) dt_rec_modif ,           
	from_unixtime(unix_timestamp(cco.fecha_baja ,'dd/MM/yyyy HH:mm:ss')) dt_rec_baja ,           
	TRIM(cco.id_zona) cod_rec_zona ,            
	TRIM(cco.estado) cod_rec_estado ,           
	case when length(TRIM(cco.red))=0 then null else TRIM(cco.red) end cod_rec_red ,
	case when length(TRIM(cco.domicilio))=0 then null else TRIM(cco.domicilio) end ds_rec_domicilio ,
	case when length(TRIM(cco.localidad))=0 then null else TRIM(cco.localidad) end ds_rec_localidad ,
	case when length(TRIM(cco.provincia))=0 then null else TRIM(cco.provincia) end ds_rec_provincia ,
	case when length(TRIM(cco.cod_postal))=0 then null else TRIM(cco.cod_postal) end cod_rec_cod_postal ,
	case when length(TRIM(cco.nro_cuenta_interna))=0 then null else TRIM(cco.nro_cuenta_interna) end cod_rec_nro_cuenta_interna ,
	case when length(TRIM(cco.nup))=0 then null else TRIM(cco.nup) end cod_per_nup ,
	'{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_STG_Cosmos_Daily') }}' partition_date
FROM
	bi_corp_staging.cosmos_centro_costos cco
INNER JOIN (
	SELECT
		CONCAT(TRIM(cod_sucursal),'-',TRIM(cod_oficina)) suc_ofi, MAX(TRIM(fecha_modif)) fecha_modif
	FROM
		bi_corp_staging.cosmos_centro_costos
	GROUP BY
		CONCAT(TRIM(cod_sucursal),'-',TRIM(cod_oficina)) ) fil ON
	CONCAT(TRIM(cco.cod_sucursal),'-',TRIM(cco.cod_oficina)) = fil.suc_ofi
	AND TRIM(cco.fecha_modif) = fil.fecha_modif;
"