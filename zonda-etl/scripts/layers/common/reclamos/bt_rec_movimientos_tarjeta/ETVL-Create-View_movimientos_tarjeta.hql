CREATE VIEW bi_corp_common.vstk_rec_movimientos_tarjeta AS
SELECT
	gest_movi_tarjeta.id_gestion cod_rec_gestion ,
	gest_movi_tarjeta.nro_movimiento cod_rec_nro_movimiento ,
	gest_movi_tarjeta.cod_producto_altair cod_rec_producto_altair ,
	gest_movi_tarjeta.nro_cuenta cod_rec_nro_cuenta ,
	gest_movi_tarjeta.nro_tarjeta cod_rec_nro_tarjeta ,
	gest_movi_tarjeta.fecha_movimiento dt_rec_fecha_movimiento ,
	gest_movi_tarjeta.nro_comprobante cod_rec_nro_comprobante ,
	gest_movi_tarjeta.cod_autorizacion cod_rec_cod_autorizacion ,
	gest_movi_tarjeta.nro_establecimiento cod_rec_nro_establecimiento ,
	gest_movi_tarjeta.desc_movimiento ds_rec_movimiento ,
	gest_movi_tarjeta.cod_operativo cod_rec_operativo ,
	cod_oper_tarjeta.desc_movimiento ds_rec_operativo ,
	gest_movi_tarjeta.cod_moneda cod_rec_divisa_tarjeta ,
	gest_movi_tarjeta.monto_movimiento fc_rec_monto_movimiento ,
	gest_movi_tarjeta.ind_devolucion flag_rec_ind_devolucion ,
	gest_movi_tarjeta.fecha_devolucion dt_rec_fecha_devolucion ,
	gest_movi_tarjeta.fecha_exportacion dt_rec_fecha_exportacion ,
	gest_movi_tarjeta.fecha_cierre_resumen dt_rec_fecha_cierre_resumen ,
	gest_movi_tarjeta.monto_devolucion fc_rec_monto_devolucion ,
	gest_movi_tarjeta.cod_rubro cod_rec_rubro ,
	gest_movi_tarjeta.id_usuario_alta cod_rec_usuario_alta ,
	gest_movi_tarjeta.fecha_alta dt_rec_alta ,
	gest_movi_tarjeta.id_usuario_modif cod_rec_usuario_modif ,
	gest_movi_tarjeta.fecha_modif dt_rec_modif ,
	gest_movi_tarjeta.fecha_baja dt_rec_baja ,
	gest_movi_tarjeta.tasa_promedio fc_rec_tasa_promedio ,
	gest_movi_tarjeta.monto_interes fc_rec_monto_interes ,
	gest_movi_tarjeta.monto_comision fc_rec_monto_comision ,
	gest_movi_tarjeta.cant_cuota fc_rec_cantidad_cuota ,
	gest_movi_tarjeta.nro_cuota cod_rec_nro_cuota ,
	gest_movi_tarjeta.fecha_cierre_cartera dt_rec_cierre_cartera ,
	gest_movi_tarjeta.nro_cupon cod_rec_nro_cupon ,
	gest_movi_tarjeta.cant_cuotas_reclamadas fc_rec_cant_cuotas_reclamadas ,
	gest_movi_tarjeta.monto_reclamado fc_rec_monto_reclamado ,
	usuarios_alta.nombre ds_rec_usuario_alta_nombre ,
	usuarios_alta.apellido ds_rec_usuario_alta_apellido ,
	usuarios_modif.nombre ds_rec_usuario_modif_nombre ,
	usuarios_modif.apellido ds_rec_usuario_modif_apellido ,
	gest_movi_tarjeta.partition_date
FROM
	bi_corp_staging.cosmos_gest_movimientos_tarjeta gest_movi_tarjeta
LEFT JOIN bi_corp_staging.cosmos_usuarios usuarios_alta ON
	gest_movi_tarjeta.id_usuario_alta = usuarios_alta.id_usuario
LEFT JOIN bi_corp_staging.cosmos_usuarios usuarios_modif ON
	gest_movi_tarjeta.id_usuario_modif = usuarios_modif.id_usuario
LEFT JOIN bi_corp_staging.cosmos_codigos_operativos_tarjeta cod_oper_tarjeta ON
	gest_movi_tarjeta.cod_operativo=cod_oper_tarjeta.cod_operativo
WHERE
	gest_movi_tarjeta.partition_date >= date_sub(to_date(from_unixtime(unix_timestamp())),1)