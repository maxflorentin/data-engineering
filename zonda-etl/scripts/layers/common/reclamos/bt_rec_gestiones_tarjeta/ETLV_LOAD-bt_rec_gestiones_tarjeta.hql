set mapred.job.queue.name=root.dataeng;
set hive.exec.dynamic.partition.mode=nonstrict;

INSERT OVERWRITE TABLE bi_corp_common.bt_rec_gestiones_tarjeta
PARTITION(partition_date)

SELECT
	TRIM(gest_tarjeta.id_gestion) AS cod_rec_gestion ,
	TRIM(gest_tarjeta.orden_estado) AS cod_rec_orden_estado ,
	TRIM(gest_tarjeta.cod_producto_altair) AS cod_rec_producto_altair ,
	TRIM(gest_tarjeta.sucursal_cuenta) AS cod_suc_sucursal_cuenta ,
	TRIM(gest_tarjeta.nro_cuenta) AS cod_rec_nro_cuenta ,
	case when TRIM(gest_tarjeta.cod_estado_cuenta)='null' then null else TRIM(gest_tarjeta.cod_estado_cuenta) end AS cod_rec_estado_cuenta ,
	case when TRIM(gest_tarjeta.fecha_alta_cuenta)='null' then null else TRIM(gest_tarjeta.fecha_alta_cuenta) end AS ts_rec_alta_cuenta ,
	TRIM(gest_tarjeta.nro_tarjeta) AS cod_rec_nro_tarjeta ,
	case when TRIM(gest_tarjeta.meta_consumo_anual)='null' then null else TRIM(gest_tarjeta.meta_consumo_anual) end AS fc_rec_meta_consumo_anual ,
	case when TRIM(gest_tarjeta.consumo_acum_periodo_act)='null' then null else TRIM(gest_tarjeta.consumo_acum_periodo_act) end AS fc_rec_consumo_acum_periodo_act ,
	case when TRIM(gest_tarjeta.ind_titularidad)='null' then null else TRIM(gest_tarjeta.ind_titularidad) end  AS cod_rec_titular ,
	case when TRIM(gest_tarjeta.cod_producto_amas)='null' then null else TRIM(gest_tarjeta.cod_producto_amas) end AS cod_rec_producto_amas ,
	case when TRIM(gest_tarjeta.digito_verificador)='null' then null else TRIM(gest_tarjeta.digito_verificador) end AS cod_rec_digito_verificador ,
	case when TRIM(gest_tarjeta.prod_paquete_cuenta)='null' then null else TRIM(gest_tarjeta.prod_paquete_cuenta) end AS cod_rec_prod_paquete_cuenta ,
	case when TRIM(gest_tarjeta.subprod_paquete_cuenta)='null' then null else TRIM(gest_tarjeta.subprod_paquete_cuenta) end AS cod_rec_subprod_paquete_cuenta ,
	case when TRIM(gest_tarjeta.nro_cartera)='null' then null else TRIM(gest_tarjeta.nro_cartera) end AS cod_rec_nro_cartera ,
	case when TRIM(gest_tarjeta.monto_saldo_acreedor)='null' then null else TRIM(gest_tarjeta.monto_saldo_acreedor) end AS fc_rec_monto_saldo_acreedor ,
	case when TRIM(gest_tarjeta.ind_dev_acreedor)='null' then null else TRIM(gest_tarjeta.ind_dev_acreedor) end AS cod_rec_dev_acreedor ,
	case when TRIM(gest_tarjeta.fecha_dev_acreedor)='null' then null else TRIM(gest_tarjeta.fecha_dev_acreedor) end  AS ts_rec_dev_acreedor ,
	case when TRIM(gest_tarjeta.monto_dev_acreedor)='null' then null else TRIM(gest_tarjeta.monto_dev_acreedor) end AS fc_rec_monto_dev_acreedor ,
	case when TRIM(gest_tarjeta.asdo)='null' then null else TRIM(gest_tarjeta.asdo) end AS cod_rec_asdo ,
	case when TRIM(gest_tarjeta.pago_minimo)='null' then null else TRIM(gest_tarjeta.pago_minimo) end AS fc_rec_pago_minimo ,
	case when TRIM(gest_tarjeta.monto_maximo_dev)='null' then null else TRIM(gest_tarjeta.monto_maximo_dev) end AS fc_rec_monto_maximo_dev ,
	case when TRIM(gest_tarjeta.monto_parcial_dev)='null' then null else TRIM(gest_tarjeta.monto_parcial_dev) end AS fc_rec_monto_parcial_dev ,
	case when TRIM(gest_tarjeta.cod_motivo_baja_cta)='null' then null else TRIM(gest_tarjeta.cod_motivo_baja_cta) end AS cod_rec_motivo_baja_cta ,
	case when TRIM(gest_tarjeta.monto_dev_acreedor_usd)='null' then null else TRIM(gest_tarjeta.monto_dev_acreedor_usd) end AS fc_rec_monto_dev_acreedor_usd ,
	case when TRIM(gest_tarjeta.monto_saldo_acreedor_usd)='null' then null else TRIM(gest_tarjeta.monto_saldo_acreedor_usd) end AS fc_rec_monto_saldo_acreedor_usd ,
	case when TRIM(gest_tarjeta.fecha_dev_acreedor_usd)='null' then null else TRIM(gest_tarjeta.fecha_dev_acreedor_usd) end AS ts_rec_dev_acreedor_usd ,
	case when TRIM(gest_tarjeta.ind_dev_acreedor_usd)='null' then null else TRIM(gest_tarjeta.ind_dev_acreedor_usd) end AS cod_rec_dev_acreedor_usd ,
	case when TRIM(gest_tarjeta.id_usuario_alta)='null' then null else TRIM(gest_tarjeta.id_usuario_alta) end AS cod_rec_usuario_alta ,
	case when TRIM(gest_tarjeta.fecha_alta)='null' then null else TRIM(gest_tarjeta.fecha_alta) end AS ts_rec_alta ,
	case when TRIM(gest_tarjeta.id_usuario_modif)='null' then null else TRIM(gest_tarjeta.id_usuario_modif) end AS cod_rec_usuario_modif ,
	case when TRIM(gest_tarjeta.fecha_modif)='null' then null else TRIM(gest_tarjeta.fecha_modif) end AS ts_rec_modif ,
	case when TRIM(gest_tarjeta.fecha_baja)='null' then null else TRIM(gest_tarjeta.fecha_baja) end  AS ts_rec_baja ,
	case when TRIM(gest_tarjeta.fecha_ultimo_cierre)='null' then null else TRIM(gest_tarjeta.fecha_ultimo_cierre) end  AS ts_rec_ultimo_cierre ,
	CASE WHEN TRIM(gest_tarjeta.ind_trj_denunciante)='S' then 1 else 0 end  AS flag_rec_trj_denunciante,
	CASE WHEN TRIM(gest_tarjeta.ind_chip)='S' then 1 else 0 end  AS flag_rec_chip,
	case when TRIM(gest_tarjeta.nup_titular_tarjeta)='null' then null else TRIM(gest_tarjeta.nup_titular_tarjeta) end AS cod_rec_nup_titular_tarjeta ,
	CASE WHEN TRIM(gest_tarjeta.ind_contacto_comercio)='S' then 1 else 0 end  AS flag_rec_contacto_comercio ,
	usuarios_alta.ds_rec_usuario_apellido AS ds_rec_usuario_alta_apellido ,
	usuarios_modif.ds_rec_usuario_nombre AS ds_rec_usuario_modif_nombre ,
	usuarios_modif.ds_rec_usuario_nombre AS ds_rec_usuario_modif_nombre ,
	usuarios_modif.ds_rec_usuario_apellido AS ds_rec_usuario_modif_apellido ,
	gest_tarjeta.partition_date
FROM
	bi_corp_staging.rio187_gest_tarjeta gest_tarjeta
LEFT JOIN bi_corp_common.dim_rec_usuario usuarios_alta ON
	TRIM(gest_tarjeta.id_usuario_alta) = usuarios_alta.cod_rec_usuario 
LEFT JOIN bi_corp_common.dim_rec_usuario usuarios_modif ON
	TRIM(gest_tarjeta.id_usuario_modif) = usuarios_modif.cod_rec_usuario
WHERE gest_tarjeta.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Cosmos') }}'
and  to_date(TRIM(gest_tarjeta.fecha_alta))= '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Cosmos') }}';