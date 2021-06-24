set mapred.job.queue.name=root.dataeng;

CREATE VIEW bi_corp_common.vstk_rec_gestiones_estados AS
SELECT
	gest_estados.id_gestion cod_rec_gestion ,
	gest_estados.orden_estado cod_rec_orden_estado ,
	gest_estados.id_estado cod_rec_estado ,
	gest_estados.fecha_estado dt_rec_estado ,
	gest_estados.id_actor cod_rec_actor ,
	gest_estados.id_usuario cod_rec_usuario ,
	gest_estados.comentario ds_rec_comentario ,
	gest_estados.id_usuario_alta cod_rec_usuario_alta ,
	gest_estados.fecha_alta dt_rec_alta ,
	gest_estados.id_usuario_modif cod_rec_usuario_modif ,
	gest_estados.fecha_modif dt_rec_modif ,
	gest_estados.fecha_baja dt_rec_baja ,
	usuarios_alta.nombre ds_rec_usuario_alta_nombre ,
	usuarios_alta.apellido ds_rec_usuario_alta_apellido ,
	usuarios_modif.nombre ds_rec_usuario_modif_nombre ,
	usuarios_modif.apellido ds_rec_usuario_modif_apellido ,
	gest_estados.partition_date partition_date
FROM
	bi_corp_staging.cosmos_gest_estados gest_estados
LEFT JOIN bi_corp_staging.cosmos_usuarios usuarios_alta ON
	gest_estados.id_usuario_alta = usuarios_alta.id_usuario
LEFT JOIN bi_corp_staging.cosmos_usuarios usuarios_modif ON
	gest_estados.id_usuario_modif = usuarios_modif.id_usuario
WHERE
	gest_estados.partition_date >= date_sub(TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP())),1);