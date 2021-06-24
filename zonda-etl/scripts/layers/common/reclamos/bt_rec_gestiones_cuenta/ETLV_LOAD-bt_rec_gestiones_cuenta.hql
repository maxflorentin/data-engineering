set mapred.job.queue.name=root.dataeng;
set hive.exec.dynamic.partition.mode=nonstrict;

INSERT OVERWRITE TABLE bi_corp_common.bt_rec_gestiones_cuenta
PARTITION(partition_date)
SELECT
	TRIM(gest_cuenta.id_gestion) AS cod_rec_gestion ,
	TRIM(gest_cuenta.orden_estado) AS cod_rec_orden_estado ,
	TRIM(gest_cuenta.sucursal_cuenta) AS cod_suc_sucursal_cuenta ,
	TRIM(gest_cuenta.nro_cuenta) AS cod_cue_cuenta ,
	case when length(trim(gest_cuenta.divisa_cuenta))=0 then null else trim(gest_cuenta.divisa_cuenta) end AS cod_rec_divisa_cuenta ,
	TRIM(gest_cuenta.aplicacion_cuenta) AS ds_rec_aplicacion_cuenta ,
	case when length(trim(gest_cuenta.cod_moneda))=0 or trim(gest_cuenta.cod_moneda)='null' then null else trim(gest_cuenta.cod_moneda) end AS cod_rec_moneda ,
	case when length(trim(gest_cuenta.costo_cuenta))=0 or trim(gest_cuenta.costo_cuenta)='null'  then null else trim(gest_cuenta.costo_cuenta) end AS fc_rec_costo_cuenta ,
	case when length(trim(gest_cuenta.nro_convenio))=0 or trim(gest_cuenta.nro_convenio)='null'  then null else trim(gest_cuenta.nro_convenio) end AS cod_rec_nro_convenio ,
	case when length(trim(gest_cuenta.cod_convenio))=0 or trim(gest_cuenta.cod_convenio)='null'  then null else trim(gest_cuenta.cod_convenio) end AS cod_rec_convenio ,
	case when length(trim(gest_cuenta.monto_convenio))=0 or trim(gest_cuenta.monto_convenio)='null'  then null else trim(gest_cuenta.monto_convenio) end AS fc_rec_monto_convenio ,
	case when length(trim(gest_cuenta.suscriptor))=0 or trim(gest_cuenta.suscriptor)='null'  then null else trim(gest_cuenta.suscriptor) end AS cod_rec_suscriptor ,
	case when length(trim(gest_cuenta.cod_campania))=0 or trim(gest_cuenta.cod_campania)='null'  then null else trim(gest_cuenta.cod_campania) end AS cod_rec_campania ,
	case when length(trim(gest_cuenta.monto_campania))=0 or trim(gest_cuenta.monto_campania)='null'  then null else trim(gest_cuenta.monto_campania) end AS fc_rec_monto_campania ,
	case when length(trim(gest_cuenta.cod_campania_recl))=0 or trim(gest_cuenta.cod_campania_recl)='null'  then null else trim(gest_cuenta.cod_campania_recl) end AS cod_rec_campania_recl ,
	case when length(trim(gest_cuenta.cod_convenio_univ))=0 or trim(gest_cuenta.cod_convenio_univ)='null'  then null else trim(gest_cuenta.cod_convenio_univ) end AS cod_rec_convenio_univ ,
	case when length(trim(gest_cuenta.cod_universidad))=0 or trim(gest_cuenta.cod_universidad)='null'  then null else trim(gest_cuenta.cod_universidad) end AS cod_rec_universidad ,
	case when length(trim(gest_cuenta.monto_convenio_univ))=0 or trim(gest_cuenta.monto_convenio_univ)='null'  then null else trim(gest_cuenta.monto_convenio_univ) end AS fc_rec_monto_covenio_univ ,
	case when trim(gest_cuenta.fecha_desde_campania)='null' then null else TRIM(gest_cuenta.fecha_desde_campania) end AS ts_rec_desde_campania ,
	case when length(trim(gest_cuenta.desc_universidad))=0 or trim(gest_cuenta.desc_universidad)='null'  then null else trim(gest_cuenta.desc_universidad) end AS ds_rec_universidad ,
	case when length(trim(gest_cuenta.cod_producto))=0 or trim(gest_cuenta.cod_producto)='null'  then null else trim(gest_cuenta.cod_producto) end AS cod_rec_producto ,
	case when length(trim(gest_cuenta.cod_subproducto))=0 or trim(gest_cuenta.cod_subproducto)='null'  then null else trim(gest_cuenta.cod_subproducto) end AS cod_rec_subproducto ,
	CASE WHEN TRIM(gest_cuenta.es_cuenta_defecto)='S' then 1 else 0 end AS flag_rec_cuenta_defecto ,
	case when trim(gest_cuenta.fecha_baja)='null' then null else TRIM(gest_cuenta.fecha_baja) end AS ts_rec_baja ,
	case when trim(gest_cuenta.fecha_alta)='null' then null else TRIM(gest_cuenta.fecha_alta) end AS ts_rec_alta ,
	case when trim(gest_cuenta.id_usuario_alta)='null' then null else TRIM(gest_cuenta.id_usuario_alta) end AS cod_rec_id_usuario_alta ,
	case when trim(gest_cuenta.id_usuario_modif)='null' then null else TRIM(gest_cuenta.id_usuario_modif) end AS cod_rec_id_usuario_modif ,
	case when trim(gest_cuenta.fecha_modif)='null' then null else TRIM(gest_cuenta.fecha_modif) end ts_rec_modif ,
	case when trim(gest_cuenta.fecha_hasta_campania)='null' then null else TRIM(gest_cuenta.fecha_desde_campania) end AS ts_rec_hasta_campania ,
	CASE WHEN TRIM(gest_cuenta.ind_tiene_convenio)='S' then 1 else 0 end AS flag_rec_tiene_convenio ,
	case when length(trim(gest_cuenta.nro_tarjeta))=0 or trim(gest_cuenta.nro_tarjeta)='null' then null else trim(gest_cuenta.nro_tarjeta) end AS cod_tcr_tarjeta ,
	CASE WHEN TRIM(gest_cuenta.ind_trj_denunciante)='S' then 1 else 0 end AS flag_rec_trj_denunciante ,
	CASE WHEN TRIM(gest_cuenta.ind_chip)='S' then 1 else 0 end  AS flag_rec_chip ,
	case when length(trim(gest_cuenta.nup_titular_tarjeta))=0  or trim(gest_cuenta.nup_titular_tarjeta)='null'  then null else trim(gest_cuenta.nup_titular_tarjeta) end AS cod_rec_nup_titular_tarjeta ,
	CASE WHEN TRIM(gest_cuenta.ind_contacto_comercio)='S' then 1 else 0 end AS flag_rec_contacto_comercio ,
	TRIM(gest_cuenta.v_estado_relacion) AS cod_rec_estado_relacion ,
	case when length(trim(gest_cuenta.id_requerimiento_premiacion))=0  or trim(gest_cuenta.id_requerimiento_premiacion)='null'  then null else trim(gest_cuenta.id_requerimiento_premiacion) end AS cod_rec_requerimiento_premiacion ,
	TRIM(gest_cuenta.sucursal_cuenta_destino) AS cod_rec_sucusal_cuenta_destino ,
	TRIM(gest_cuenta.nro_cuenta_destino) AS cod_rec_nro_cuenta_destino ,
	case when length(trim(gest_cuenta.nro_convenio_recl))=0  or trim(gest_cuenta.nro_convenio_recl)='null'  then null else trim(gest_cuenta.nro_convenio_recl) end AS cod_rec_nro_convenio_recl ,
	case when length(trim(gest_cuenta.cod_convenio_recl))=0  or trim(gest_cuenta.cod_convenio_recl)='null'  then null else trim(gest_cuenta.cod_convenio_recl) end AS cod_rec_convenio_recl ,
	case when length(trim(gest_cuenta.suscriptor_recl))=0  or trim(gest_cuenta.suscriptor_recl)='null'  then null else trim(gest_cuenta.suscriptor_recl) end AS cod_rec_suscriptor_recl ,
	case when length(trim(gest_cuenta.monto_convenio_recl))=0  or trim(gest_cuenta.monto_convenio_recl)='null'  then null else trim(gest_cuenta.monto_convenio_recl) end AS fc_rec_monto_convenio_recl ,
	case when trim(gest_cuenta.aplicacion_cuenta_destino)='null' then null else TRIM(gest_cuenta.aplicacion_cuenta_destino) end AS ds_rec_aplicacion_cuenta_destino ,
	usuarios_alta.ds_rec_usuario_nombre AS ds_rec_usuario_alta_nombre ,
	usuarios_alta.ds_rec_usuario_apellido AS ds_rec_usuario_alta_apellido ,
	usuarios_modif.ds_rec_usuario_nombre AS ds_rec_usuario_modif_nombre ,
	usuarios_modif.ds_rec_usuario_apellido AS ds_rec_usuario_modif_apellido ,
	gest_cuenta.partition_date
FROM
	bi_corp_staging.rio187_gest_cuenta gest_cuenta
LEFT JOIN bi_corp_common.dim_rec_usuario usuarios_alta ON
	TRIM(gest_cuenta.id_usuario_alta) = usuarios_alta.cod_rec_usuario 
LEFT JOIN bi_corp_common.dim_rec_usuario usuarios_modif ON
	TRIM(gest_cuenta.id_usuario_modif) = usuarios_modif.cod_rec_usuario
WHERE gest_cuenta.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Cosmos') }}'
and to_date(gest_cuenta.fecha_alta) = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Cosmos') }}'
;
