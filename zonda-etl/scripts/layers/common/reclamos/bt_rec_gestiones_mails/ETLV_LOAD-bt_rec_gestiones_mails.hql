SET mapred.job.queue.name=root.dataeng;
set hive.exec.dynamic.partition.mode=nonstrict;

INSERT OVERWRITE TABLE bi_corp_common.bt_rec_gestiones_mails
PARTITION(partition_date)
SELECT
		TRIM(gest_mails.id_gestion) cod_rec_gestion ,
		TRIM(gest_mails.orden_estado) cod_rec_orden_estado ,
		case when LOWER(TRIM(gest_mails.email))='null' then null else LOWER(TRIM(gest_mails.email)) end ds_rec_email ,
		LOWER(TRIM(gest_mails.asunto_mail)) ds_rec_asunto_mail ,
		LOWER(TRIM(gest_mails.texto_mail)) ds_rec_texto_mail ,
		TRIM(gest_mails.estado_mail) cod_rec_estado_mail ,
		TRIM(gest_mails.fecha_alta) ts_rec_alta ,
		TRIM(gest_mails.fecha_envio) ts_rec_envio ,
		UPPER(TRIM(gest_mails.tipo_mail)) ds_rec_tipo_mail ,
		TRIM(gest_mails.id_usuario_alta) cod_rec_usuario_alta ,
		TRIM(gest_mails.id_usuario_modif) cod_rec_usuario_modif ,
		TRIM(gest_mails.fecha_modif) ts_rec_modif ,
		TRIM(gest_mails.fecha_baja) ts_rec_baja ,
		TRIM(gest_mails.nro_envio) cod_rec_nro_envio ,
		--TRIM(gest_mails.m_automatico) flag_rec_automatico,
		--gest_mails.m_automatico,
		CASE WHEN TRIM(gest_mails.m_automatico)='S' then 1 else 0 end flag_rec_automatico ,
		UPPER(TRIM(usuarios_alta.ds_rec_usuario_nombre)) ds_rec_usuario_alta_nombre ,
		UPPER(TRIM(usuarios_alta.ds_rec_usuario_apellido)) ds_rec_usuario_alta_apellido ,
		UPPER(TRIM(usuarios_modif.ds_rec_usuario_nombre)) ds_rec_usuario_modif_nombre ,
		UPPER(TRIM(usuarios_modif.ds_rec_usuario_apellido)) ds_rec_usuario_modif_apellido ,
		gest_mails.partition_date
FROM
		bi_corp_staging.rio187_gest_mails gest_mails
LEFT JOIN bi_corp_common.dim_rec_usuario usuarios_alta ON
		TRIM(gest_mails.id_usuario_alta) = usuarios_alta.cod_rec_usuario
LEFT JOIN bi_corp_common.dim_rec_usuario usuarios_modif ON
		TRIM(gest_mails.id_usuario_modif) = usuarios_modif.cod_rec_usuario
WHERE gest_mails.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Cosmos') }}'
	  and to_date(TRIM(gest_mails.fecha_modif)) = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Cosmos') }}'