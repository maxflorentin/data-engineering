"
SET mapred.job.queue.name=root.dataeng;
set hive.exec.dynamic.partition.mode=nonstrict;

INSERT
	OVERWRITE TABLE bi_corp_common.dim_rec_actor
SELECT
	TRIM(ori.id_actor) cod_rec_actor ,
	TRIM(ori.tipo_actor) cod_rec_tipo_actor ,
	TRIM(UPPER(ori.desc_actor))  ds_rec_actor ,
	TRIM(ori.id_usuario_alta) cod_rec_usuario_alta ,
	from_unixtime(unix_timestamp(ori.fecha_alta,'dd/MM/yyyy HH:mm:ss')) dt_rec_actor_alta ,
	TRIM(ori.id_usuario_modif) cod_rec_usuario_modif ,
	from_unixtime(unix_timestamp(ori.fecha_modif,'dd/MM/yyyy HH:mm:ss')) dt_rec_actor_modif ,
	ori.partition_date partition_date 
FROM
	bi_corp_staging.rio187_actores ori
INNER JOIN (
	SELECT
		TRIM(id_actor) id_actor , MAX(from_unixtime(unix_timestamp(fecha_modif,'dd/MM/yyyy HH:mm:ss'))) fecha_modif
	FROM
		bi_corp_staging.cosmos_actores
	GROUP BY
		TRIM(id_actor)) fil ON
	TRIM(ori.id_actor) = fil.id_actor
	AND from_unixtime(unix_timestamp(ori.fecha_modif,'dd/MM/yyyy HH:mm:ss')) = fil.fecha_modif ;
"