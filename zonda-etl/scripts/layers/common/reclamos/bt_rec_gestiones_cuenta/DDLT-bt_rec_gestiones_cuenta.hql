CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_common.bt_rec_gestiones_cuenta (
    cod_rec_gestion INT ,
    cod_rec_orden_estado INT ,
    cod_suc_sucursal_cuenta INT ,
    cod_cue_cuenta INT ,
    cod_rec_divisa_cuenta STRING ,
    ds_rec_aplicacion_cuenta STRING ,
    cod_rec_moneda STRING ,
    fc_rec_costo_cuenta DECIMAL(15,2) ,
    cod_rec_nro_convenio INT ,
    cod_rec_convenio STRING ,
    fc_rec_monto_convenio INT ,
    cod_rec_suscriptor STRING ,
    cod_rec_campania STRING ,
    fc_rec_monto_campania DECIMAL(15,2) ,
    cod_rec_campania_recl STRING ,
    cod_rec_convenio_univ INT ,
    cod_rec_universidad STRING ,
    fc_rec_monto_covenio_univ DECIMAL(15,2) ,
    ts_rec_desde_campania TIMESTAMP ,
    ds_rec_universidad STRING ,
    cod_rec_producto INT ,
    cod_rec_subproducto STRING ,
    flag_rec_cuenta_defecto INT ,
    ts_rec_baja TIMESTAMP ,
    ts_rec_alta TIMESTAMP ,
    cod_rec_id_usuario_alta STRING ,
    cod_rec_id_usuario_modif STRING ,
    ts_rec_modif TIMESTAMP ,
    ts_rec_hasta_campania TIMESTAMP ,
    flag_rec_tiene_convenio INT ,
    cod_rec_nro_tarjeta STRING ,
	flag_rec_trj_denunciante INT ,
    flag_rec_chip INT ,
    cod_rec_nup_titular_tarjeta STRING ,
    flag_rec_contacto_comercio INT ,
    cod_rec_estado_relacion STRING ,
    cod_rec_requerimiento_premiacion INT ,
    cod_suc_sucusal_cuenta_destino STRING ,
    cod_cue_cuenta_destino INT ,
    cod_rec_nro_convenio_recl INT ,
    cod_rec_convenio_recl STRING ,
    cod_rec_suscriptor_recl STRING ,
    fc_rec_monto_convenio_recl DECIMAL(15,2) ,
    ds_rec_aplicacion_cuenta_destino STRING ,
    ds_rec_usuario_alta_nombre STRING ,
    ds_rec_usuario_alta_apellido STRING ,
    ds_rec_usuario_modif_nombre STRING ,
    ds_rec_usuario_modif_apellido STRING 
)
PARTITIONED BY (
    partition_date string)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  '${DATA_LAKE_SERVER}/santander/bi-corp/common/reclamos/bt_rec_gestiones_cuenta'