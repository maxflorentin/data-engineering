set mapred.job.queue.name=root.dataeng;
set hive.exec.dynamic.partition.mode=nonstrict;

INSERT OVERWRITE TABLE bi_corp_common.bt_rec_gest_estados_csm
PARTITION(partition_date)
SELECT
	TRIM(gest_estados.id_gestion) cod_rec_gestion ,
	TRIM(gest_estados.orden_estado) cod_rec_orden_estado ,
	TRIM(gest_estados.id_estado) cod_rec_estado ,
	to_date(gest_estados.fecha_estado) dt_rec_estado,
	TRIM(gest_estados.id_actor) cod_rec_actor ,
	case when length(trim(gest_estados.id_usuario))=0 or trim(gest_estados.id_usuario)='null' then null else trim(gest_estados.id_usuario) end AS cod_rec_usuario ,
	case when length(trim(gest_estados.comentario))=0 or trim(gest_estados.comentario)='null' then null else trim(gest_estados.comentario) end AS ds_rec_comentario ,
	case when length(trim(gest_estados.id_usuario_alta))=0 or trim(gest_estados.id_usuario_alta)='null' then null else trim(gest_estados.id_usuario_alta) end AS cod_rec_usuario_alta ,
	to_date(gest_estados.fecha_alta) dt_rec_alta,
	case when length(trim(gest_estados.id_usuario_modif))=0 or trim(gest_estados.id_usuario_modif)='null' then null else trim(gest_estados.id_usuario_modif) end AS cod_rec_usuario_modif ,
	to_date(gest_estados.fecha_modif)  dt_rec_modif,
	to_date(gest_estados.fecha_baja) dt_rec_baja,
	usuarios_alta.ds_rec_usuario_nombre ds_rec_usuario_alta_nombre ,
	usuarios_alta.ds_rec_usuario_apellido ds_rec_usuario_alta_apellido ,
	usuarios_modif.ds_rec_usuario_nombre ds_rec_usuario_modif_nombre ,
	usuarios_modif.ds_rec_usuario_apellido ds_rec_usuario_modif_apellido ,
	gest_estados.partition_date 
FROM
	bi_corp_staging.rio187_gest_estados gest_estados
LEFT JOIN bi_corp_common.dim_rec_usuario usuarios_alta ON
	TRIM(gest_estados.id_usuario_alta) = usuarios_alta.cod_rec_usuario 
LEFT JOIN bi_corp_common.dim_rec_usuario usuarios_modif ON
	TRIM(gest_estados.id_usuario_modif) = usuarios_modif.cod_rec_usuario
WHERE gest_estados.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='max_partition_rio187_gest_estados', dag_id='LOAD_CMN_Cosmos') }}'
and gest_estados.partition_date=to_date(gest_estados.fecha_alta) -- Cargo Novedades
;
