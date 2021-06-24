"
SET mapred.job.queue.name=root.dataeng;
set hive.exec.dynamic.partition.mode=nonstrict;

INSERT OVERWRITE TABLE bi_corp_common.bt_rec_movimientos_tarjeta
PARTITION(partition_date)
SELECT
	TRIM(gest_movi_tarjeta.id_gestion) cod_rec_gestion ,
	TRIM(gest_movi_tarjeta.nro_movimiento) cod_rec_nro_movimiento ,
	TRIM(gest_movi_tarjeta.cod_producto_altair) cod_rec_producto_altair ,
	TRIM(gest_movi_tarjeta.nro_cuenta) cod_rec_nro_cuenta ,
	TRIM(gest_movi_tarjeta.nro_tarjeta) cod_rec_nro_tarjeta ,
	from_unixtime(unix_timestamp(TRIM(gest_movi_tarjeta.fecha_movimiento) , 'dd/MM/yyyy HH:mm:ss'), 'yyyy-MM-dd') dt_rec_fecha_movimiento,
	TRIM(gest_movi_tarjeta.nro_comprobante) cod_rec_nro_comprobante ,
	case when length(TRIM(gest_movi_tarjeta.cod_autorizacion))=0 then null else TRIM(gest_movi_tarjeta.cod_autorizacion) end as cod_rec_cod_autorizacion ,
	case when length(TRIM(gest_movi_tarjeta.nro_establecimiento))=0 then null else TRIM(gest_movi_tarjeta.nro_establecimiento) end  as cod_rec_nro_establecimiento ,
	case when length(TRIM(gest_movi_tarjeta.desc_movimiento))=0 then null else TRIM(gest_movi_tarjeta.desc_movimiento) end  as ds_rec_movimiento ,
	TRIM(gest_movi_tarjeta.cod_operativo) cod_rec_operativo ,
	TRIM(cod_oper_tarjeta.desc_movimiento) ds_rec_operativo ,
	TRIM(gest_movi_tarjeta.cod_moneda) cod_rec_divisa_tarjeta ,
	TRIM(gest_movi_tarjeta.monto_movimiento) fc_rec_monto_movimiento ,
	TRIM(gest_movi_tarjeta.ind_devolucion) cod_rec_ind_devolucion ,
	from_unixtime(unix_timestamp(TRIM(gest_movi_tarjeta.fecha_devolucion) ,'dd/MM/yyyy HH:mm:ss')) ts_rec_fecha_devolucion ,
	from_unixtime(unix_timestamp(TRIM(gest_movi_tarjeta.fecha_exportacion) ,'dd/MM/yyyy HH:mm:ss')) ts_rec_fecha_exportacion ,
	from_unixtime(unix_timestamp(TRIM(gest_movi_tarjeta.fecha_cierre_resumen) ,'dd/MM/yyyy HH:mm:ss')) ts_rec_fecha_cierre_resumen ,
	TRIM(gest_movi_tarjeta.monto_devolucion) fc_rec_monto_devolucion ,
	case when length(TRIM(gest_movi_tarjeta.cod_rubro))=0 then null else TRIM(gest_movi_tarjeta.cod_rubro) end  as cod_rec_rubro ,
	TRIM(gest_movi_tarjeta.id_usuario_alta) cod_rec_usuario_alta ,
	from_unixtime(unix_timestamp(TRIM(gest_movi_tarjeta.fecha_alta) ,'dd/MM/yyyy HH:mm:ss')) ts_rec_alta ,
	TRIM(gest_movi_tarjeta.id_usuario_modif) cod_rec_usuario_modif ,
	from_unixtime(unix_timestamp(TRIM(gest_movi_tarjeta.fecha_modif) ,'dd/MM/yyyy HH:mm:ss')) ts_rec_modif ,
	from_unixtime(unix_timestamp(TRIM(gest_movi_tarjeta.fecha_baja) ,'dd/MM/yyyy HH:mm:ss')) ts_rec_baja ,
	case when length(TRIM(gest_movi_tarjeta.tasa_promedio))=0 then null else TRIM(gest_movi_tarjeta.tasa_promedio) end  as fc_rec_tasa_promedio ,
	case when length(TRIM(gest_movi_tarjeta.monto_interes))=0 then null else TRIM(gest_movi_tarjeta.monto_interes) end  as fc_rec_monto_interes ,
	case when length(TRIM(gest_movi_tarjeta.monto_comision))=0 then null else TRIM(gest_movi_tarjeta.monto_comision) end  as fc_rec_monto_comision ,
	case when length(TRIM(gest_movi_tarjeta.cant_cuota))=0 then null else TRIM(gest_movi_tarjeta.cant_cuota) end  as fc_rec_cantidad_cuota ,
	case when length(TRIM(gest_movi_tarjeta.nro_cuota))=0 then null else TRIM(gest_movi_tarjeta.nro_cuota) end  as cod_rec_nro_cuota ,
	from_unixtime(unix_timestamp(TRIM(gest_movi_tarjeta.fecha_cierre_cartera) , 'dd/MM/yyyy HH:mm:ss'), 'yyyy-MM-dd') dt_rec_cierre_cartera,
	case when length(TRIM(gest_movi_tarjeta.nro_cupon))=0 then null else TRIM(gest_movi_tarjeta.nro_cupon) end  as cod_rec_nro_cupon ,
	case when length(TRIM(gest_movi_tarjeta.cant_cuotas_reclamadas))=0 then null else TRIM(gest_movi_tarjeta.cant_cuotas_reclamadas) end  as fc_rec_cant_cuotas_reclamadas ,
	TRIM(gest_movi_tarjeta.monto_reclamado) fc_rec_monto_reclamado ,
	TRIM(usuarios_alta.ds_rec_usuario_nombre) ds_rec_usuario_alta_nombre ,
	TRIM(usuarios_alta.ds_rec_usuario_apellido) ds_rec_usuario_alta_apellido ,
	TRIM(usuarios_modif.ds_rec_usuario_nombre) ds_rec_usuario_modif_nombre ,
	TRIM(usuarios_modif.ds_rec_usuario_apellido) ds_rec_usuario_modif_apellido ,
	gest_movi_tarjeta.partition_date
FROM
	bi_corp_staging.cosmos_gest_movimientos_tarjeta gest_movi_tarjeta
LEFT JOIN bi_corp_common.dim_rec_usuario usuarios_alta ON
	TRIM(gest_movi_tarjeta.id_usuario_alta) = usuarios_alta.cod_rec_usuario
LEFT JOIN bi_corp_common.dim_rec_usuario usuarios_modif ON
	TRIM(gest_movi_tarjeta.id_usuario_modif) = usuarios_modif.cod_rec_usuario
LEFT JOIN bi_corp_staging.cosmos_codigos_operativos_tarjeta cod_oper_tarjeta ON
	TRIM(gest_movi_tarjeta.cod_operativo) = TRIM(cod_oper_tarjeta.cod_operativo)
WHERE
	gest_movi_tarjeta.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Cosmos') }}'
"