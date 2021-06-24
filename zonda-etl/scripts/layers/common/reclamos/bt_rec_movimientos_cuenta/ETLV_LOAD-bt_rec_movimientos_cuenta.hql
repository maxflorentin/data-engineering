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
	to_date(TRIM(gest_movi_cuenta.fecha_movimiento)) dt_rec_fecha_movimiento,
	TRIM(gest_movi_cuenta.monto_movimiento) fc_rec_monto_movimiento ,
	TRIM(gest_movi_cuenta.ind_devolucion) cod_rec_ind_devolucion ,
	case when TRIM(gest_movi_cuenta.fecha_devolucion)='null' then null else TRIM(gest_movi_cuenta.fecha_devolucion) end ts_rec_fecha_devolucion,
	case when TRIM(gest_movi_cuenta.monto_devolucion)='null' then null else TRIM(gest_movi_cuenta.monto_devolucion) end fc_rec_monto_devolucion ,
	case when TRIM(gest_movi_cuenta.cod_rubro)='null' or length(trim(gest_movi_cuenta.cod_rubro))=0  then null else TRIM(gest_movi_cuenta.cod_rubro) end cod_rec_cod_rubro ,
	case when TRIM(gest_movi_cuenta.nro_establecimiento)='null' or length(trim(gest_movi_cuenta.nro_establecimiento))=0 then null else TRIM(gest_movi_cuenta.nro_establecimiento) end cod_rec_nro_establecimiento ,
	case when TRIM(gest_movi_cuenta.id_usuario_alta)='null' then null else TRIM(gest_movi_cuenta.id_usuario_alta) end cod_rec_id_usuario_alta ,
	case when TRIM(gest_movi_cuenta.fecha_alta)='null' then null else to_date(gest_movi_cuenta.fecha_alta) end dt_rec_fecha_alta ,
	case when TRIM(gest_movi_cuenta.id_usuario_modif)='null' then null else TRIM(gest_movi_cuenta.id_usuario_modif) end cod_rec_id_usuario_modif ,
	case when TRIM(gest_movi_cuenta.fecha_modif)='null' then null else TRIM(gest_movi_cuenta.fecha_modif) end ts_rec_fecha_modif ,
    case when TRIM(gest_movi_cuenta.fecha_baja)='null' then null else TRIM(gest_movi_cuenta.fecha_baja) end ts_rec_fecha_baja ,
	case when length(trim(gest_movi_cuenta.nombre_establecimiento))=0 or TRIM(gest_movi_cuenta.nombre_establecimiento)='null' then null else trim(gest_movi_cuenta.nombre_establecimiento) end AS ds_rec_nombre_establecimiento ,
	case when length(trim(gest_movi_cuenta.cod_autorizacion))=0 or TRIM(gest_movi_cuenta.cod_autorizacion)='null' then null else trim(gest_movi_cuenta.cod_autorizacion) end AS cod_rec_cod_autorizacion ,
	case when length(trim(gest_movi_cuenta.nro_cupon))=0 or TRIM(gest_movi_cuenta.nro_cupon)='null' then null else trim(gest_movi_cuenta.nro_cupon) end AS cod_rec_nro_cupon ,
	case when length(trim(gest_movi_cuenta.nombre_empresa))=0 or TRIM(gest_movi_cuenta.nombre_empresa)='null' then null else trim(gest_movi_cuenta.nombre_empresa) end AS ds_rec_nombre_empresa ,
	case when length(trim(gest_movi_cuenta.empresa_b24))=0 or TRIM(gest_movi_cuenta.empresa_b24)='null' then null else trim(gest_movi_cuenta.empresa_b24) end AS ds_rec_empresa_b24 ,
	case when TRIM(gest_movi_cuenta.monto_reclamado)='null' then null else TRIM(gest_movi_cuenta.monto_reclamado) end fc_rec_monto_reclamado ,
	case when length(trim(gest_movi_cuenta.tasa_promedio))=0 or TRIM(gest_movi_cuenta.tasa_promedio)='null' then null else trim(gest_movi_cuenta.tasa_promedio) end AS fc_rec_tasa_promedio ,
	case when length(trim(gest_movi_cuenta.monto_interes))=0 or TRIM(gest_movi_cuenta.monto_interes)='null' then null else trim(gest_movi_cuenta.monto_interes) end AS fc_rec_monto_interes ,
	case when TRIM(gest_movi_cuenta.monto_comision)='null' then null else TRIM(gest_movi_cuenta.monto_comision) end  fc_rec_monto_comision ,
	case when length(trim(gest_movi_cuenta.id_motivo))=0 or TRIM(gest_movi_cuenta.id_motivo)='null' then null else trim(gest_movi_cuenta.id_motivo) end AS cod_rec_motivo ,
	case when length(trim(gest_movi_cuenta.cod_op_brio_grupo))=0 or TRIM(gest_movi_cuenta.cod_op_brio_grupo)='null' then null else trim(gest_movi_cuenta.cod_op_brio_grupo) end AS cod_rec_cod_op_brio_grupo ,
	case when length(trim(gest_movi_cuenta.cod_op_brio_subgrupo))=0 or TRIM(gest_movi_cuenta.cod_op_brio_subgrupo)='null' then null else trim(gest_movi_cuenta.cod_op_brio_subgrupo) end AS cod_rec_cod_op_brio_subgrupo ,
	case when length(trim(gest_movi_cuenta.nro_tarjeta_debito))=0 or TRIM(gest_movi_cuenta.nro_tarjeta_debito)='null' then null else trim(gest_movi_cuenta.nro_tarjeta_debito) end AS cod_rec_nro_tarjeta_debito ,
	case when length(trim(gest_movi_cuenta.id_atm))=0 or TRIM(gest_movi_cuenta.id_atm)='null' then null else trim(gest_movi_cuenta.id_atm) end cod_rec_atm ,
	TRIM(usuarios_alta.ds_rec_usuario_nombre) ds_rec_usuario_alta_nombre ,
	TRIM(usuarios_alta.ds_rec_usuario_apellido) ds_rec_usuario_alta_apellido ,
	TRIM(usuarios_modif.ds_rec_usuario_nombre) ds_rec_usuario_modif_nombre ,
	TRIM(usuarios_modif.ds_rec_usuario_apellido) ds_rec_usuario_modif_apellido,
	gest_movi_cuenta.partition_date
FROM
	bi_corp_staging.rio187_gest_movimientos_cuenta gest_movi_cuenta
LEFT JOIN bi_corp_common.dim_rec_usuario usuarios_alta ON
	TRIM(gest_movi_cuenta.id_usuario_alta) = usuarios_alta.cod_rec_usuario
LEFT JOIN bi_corp_common.dim_rec_usuario usuarios_modif ON
	TRIM(gest_movi_cuenta.id_usuario_modif) = usuarios_modif.cod_rec_usuario
LEFT JOIN bi_corp_staging.cosmos_codigos_operativos_cuenta cod_oper_cuenta ON
	gest_movi_cuenta.cod_operativo = cod_oper_cuenta.cod_operativo
WHERE
	gest_movi_cuenta.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Cosmos') }}'
	and coalesce(to_date(gest_movi_cuenta.fecha_modif),to_date(gest_movi_cuenta.fecha_alta))= '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Cosmos') }}';
	