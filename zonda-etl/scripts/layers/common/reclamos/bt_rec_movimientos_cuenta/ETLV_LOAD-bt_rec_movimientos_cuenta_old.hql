"
SET mapred.job.queue.name=root.dataeng;
set hive.exec.dynamic.partition.mode=nonstrict;

INSERT OVERWRITE TABLE bi_corp_common.bt_rec_movimientos_cuenta
PARTITION(partition_date)
SELECT
	TRIM(gest_movi_cuenta.id_gestion) cod_rec_gestion ,
	TRIM(gest_movi_cuenta.nro_movimiento) cod_rec_nro_movimiento ,
	TRIM(gest_movi_cuenta.sucursal_cuenta) cod_suc_sucursal_cuenta ,
	TRIM(gest_movi_cuenta.nro_cuenta) cod_rec_nro_cuenta ,
	TRIM(gest_movi_cuenta.divisa_cuenta) cod_rec_divisa_cuenta ,
	TRIM(gest_movi_cuenta.nro_comprobante) cod_rec_nro_comprobante ,
	TRIM(gest_movi_cuenta.cod_operativo) cod_rec_cod_operativo ,
	TRIM(cod_oper_cuenta.desc_larga) ds_rec_operativo ,
	from_unixtime(unix_timestamp(TRIM(gest_movi_cuenta.fecha_movimiento) , 'dd/MM/yyyy HH:mm:ss'), 'yyyy-MM-dd') dt_rec_fecha_movimiento,
	--from_unixtime(unix_timestamp(TRIM(gest_movi_cuenta.fecha_movimiento) ,'dd/MM/yyyy HH:mm:ss')) dt_rec_fecha_movimiento ,
	TRIM(gest_movi_cuenta.monto_movimiento) fc_rec_monto_movimiento ,
	TRIM(gest_movi_cuenta.ind_devolucion) cod_rec_ind_devolucion ,
	from_unixtime(unix_timestamp(TRIM(gest_movi_cuenta.fecha_devolucion) ,'dd/MM/yyyy HH:mm:ss')) ts_rec_fecha_devolucion ,
	TRIM(gest_movi_cuenta.monto_devolucion) fc_rec_monto_devolucion ,
	case when length(trim(gest_movi_cuenta.cod_rubro))=0 then null else trim(gest_movi_cuenta.cod_rubro) end AS cod_rec_cod_rubro ,
	case when length(trim(gest_movi_cuenta.nro_establecimiento))=0 then null else trim(gest_movi_cuenta.nro_establecimiento) end AS cod_rec_nro_establecimiento ,
	TRIM(gest_movi_cuenta.id_usuario_alta) cod_rec_id_usuario_alta ,
	TRIM(gest_movi_cuenta.fecha_alta) dt_rec_fecha_alta ,
	TRIM(gest_movi_cuenta.id_usuario_modif) cod_rec_id_usuario_modif ,
	from_unixtime(unix_timestamp(TRIM(gest_movi_cuenta.fecha_modif) ,'dd/MM/yyyy HH:mm:ss')) ts_rec_fecha_modif ,
	from_unixtime(unix_timestamp(TRIM(gest_movi_cuenta.fecha_baja) ,'dd/MM/yyyy HH:mm:ss')) ts_rec_fecha_baja ,
	case when length(trim(gest_movi_cuenta.nombre_establecimiento))=0 then null else trim(gest_movi_cuenta.nombre_establecimiento) end AS ds_rec_nombre_establecimiento ,
	case when length(trim(gest_movi_cuenta.cod_autorizacion))=0 then null else trim(gest_movi_cuenta.cod_autorizacion) end AS cod_rec_cod_autorizacion ,
	case when length(trim(gest_movi_cuenta.nro_cupon))=0 then null else trim(gest_movi_cuenta.nro_cupon) end AS cod_rec_nro_cupon ,
	case when length(trim(gest_movi_cuenta.nombre_empresa))=0 then null else trim(gest_movi_cuenta.nombre_empresa) end AS ds_rec_nombre_empresa ,
	case when length(trim(gest_movi_cuenta.empresa_b24))=0 then null else trim(gest_movi_cuenta.empresa_b24) end AS ds_rec_empresa_b24 ,
	TRIM(gest_movi_cuenta.monto_reclamado) fc_rec_monto_reclamado ,
	case when length(trim(gest_movi_cuenta.tasa_promedio))=0 then null else trim(gest_movi_cuenta.tasa_promedio) end AS fc_rec_tasa_promedio ,
	case when length(trim(gest_movi_cuenta.monto_interes))=0 then null else trim(gest_movi_cuenta.monto_interes) end AS fc_rec_monto_interes ,
	TRIM(gest_movi_cuenta.monto_comision) fc_rec_monto_comision ,
	case when length(trim(gest_movi_cuenta.id_motivo))=0 then null else trim(gest_movi_cuenta.id_motivo) end AS cod_rec_motivo ,
	case when length(trim(gest_movi_cuenta.cod_op_brio_grupo))=0 then null else trim(gest_movi_cuenta.cod_op_brio_grupo) end AS cod_rec_cod_op_brio_grupo ,
	case when length(trim(gest_movi_cuenta.cod_op_brio_subgrupo))=0 then null else trim(gest_movi_cuenta.cod_op_brio_subgrupo) end AS cod_rec_cod_op_brio_subgrupo ,
	case when length(trim(gest_movi_cuenta.nro_tarjeta_debito))=0 then null else trim(gest_movi_cuenta.nro_tarjeta_debito) end AS cod_rec_nro_tarjeta_debito ,
	TRIM(gest_movi_cuenta.id_atm) cod_rec_atm ,
	TRIM(usuarios_alta.ds_rec_usuario_nombre) ds_rec_usuario_alta_nombre ,
	TRIM(usuarios_alta.ds_rec_usuario_apellido) ds_rec_usuario_alta_apellido ,
	TRIM(usuarios_modif.ds_rec_usuario_nombre) ds_rec_usuario_modif_nombre ,
	TRIM(usuarios_modif.ds_rec_usuario_apellido) ds_rec_usuario_modif_apellido,
	gest_movi_cuenta.partition_date
FROM
	bi_corp_staging.cosmos_gest_movimientos_cuenta gest_movi_cuenta
LEFT JOIN bi_corp_common.dim_rec_usuario usuarios_alta ON
	TRIM(gest_movi_cuenta.id_usuario_alta) = usuarios_alta.cod_rec_usuario
LEFT JOIN bi_corp_common.dim_rec_usuario usuarios_modif ON
	TRIM(gest_movi_cuenta.id_usuario_modif) = usuarios_modif.cod_rec_usuario
LEFT JOIN bi_corp_staging.cosmos_codigos_operativos_cuenta cod_oper_cuenta ON
	gest_movi_cuenta.cod_operativo = cod_oper_cuenta.cod_operativo
WHERE
	gest_movi_cuenta.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Cosmos') }}'
"