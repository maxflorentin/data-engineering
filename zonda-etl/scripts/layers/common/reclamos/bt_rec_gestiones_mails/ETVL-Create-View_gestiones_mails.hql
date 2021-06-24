CREATE VIEW bi_corp_common.vstk_rec_gestiones_mails AS
SELECT
	gest_mails.id_gestion cod_rec_gestion ,
	gest_mails.orden_estado cod_rec_orden_estado ,
	gest_mails.email ds_rec_email ,
	gest_mails.asunto_mail ds_rec_asunto_mail ,
	gest_mails.texto_mail ds_rec_texto_mail ,
	gest_mails.estado_mail cod_rec_estado_mail ,
	gest_mails.fecha_alta dt_rec_alta ,
	gest_mails.fecha_envio dt_rec_envio ,
	gest_mails.tipo_mail ds_rec_tipo_mail ,
	gest_mails.id_usuario_alta cod_rec_usuario_alta ,
	gest_mails.id_usuario_modif cod_rec_usuario_modif ,
	gest_mails.fecha_modif dt_rec_modif ,
	gest_mails.fecha_baja dt_rec_baja ,
	gest_mails.nro_envio cod_rec_nro_envio ,
	gest_mails.m_automatico flag_rec_automatico ,
	usuarios_alta.nombre ds_rec_usuario_alta_nombre ,
	usuarios_alta.apellido ds_rec_usuario_alta_apellido ,
	usuarios_modif.nombre ds_rec_usuario_modif_nombre ,
	usuarios_modif.apellido ds_rec_usuario_modif_apellido ,
	gest_mails.partition_date
FROM
	bi_corp_staging.cosmos_get_mails gest_mails
LEFT JOIN bi_corp_staging.cosmos_usuarios usuarios_alta ON
	gest_mails.id_usuario_alta = usuarios_alta.id_usuario
LEFT JOIN bi_corp_staging.cosmos_usuarios usuarios_modif ON
	gest_mails.id_usuario_modif = usuarios_modif.id_usuario
WHERE
	gest_mails.partition_date >= date_sub(to_date(from_unixtime(unix_timestamp())),1)