CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_common.bt_rec_movimientos_tarjeta (
    cod_rec_gestion INT ,
    cod_rec_nro_movimiento INT ,
    cod_rec_producto_altair STRING ,
    cod_cue_cuenta INT ,
    cod_tcr_tarjeta INT ,
    dt_rec_fecha_movimiento STRING ,
    cod_rec_nro_comprobante INT ,
    cod_rec_cod_autorizacion STRING ,
    cod_rec_nro_establecimiento STRING ,
    ds_rec_movimiento STRING ,
    cod_rec_operativo STRING ,
    ds_rec_operativo STRING ,
    cod_rec_divisa_tarjeta STRING ,
    fc_rec_monto_movimiento DECIMAL(15,2) ,
    cod_rec_ind_devolucion STRING ,
    ts_rec_fecha_devolucion TIMESTAMP ,
    ts_rec_fecha_exportacion TIMESTAMP ,
    ts_rec_fecha_cierre_resumen TIMESTAMP ,
    fc_rec_monto_devolucion DECIMAL(15,2) ,
    cod_rec_rubro INT ,
    cod_rec_usuario_alta STRING ,
    dt_rec_alta TIMESTAMP ,
    cod_rec_usuario_modif STRING ,
    ts_rec_modif TIMESTAMP ,
    ts_rec_baja TIMESTAMP ,
    dec_rec_tasa_promedio DECIMAL(15,2) ,
    fc_rec_monto_interes DECIMAL(15,2) ,
    fc_rec_monto_comision DECIMAL(15,2) ,
    fc_rec_cantidad_cuota INT ,
    cod_rec_nro_cuota STRING ,
    dt_rec_cierre_cartera STRING ,
    cod_rec_nro_cupon STRING ,
    fc_rec_cant_cuotas_reclamadas INT ,
    fc_rec_monto_reclamado DECIMAL(15,2) ,
    ds_rec_usuario_alta_nombre STRING ,
    ds_rec_usuario_alta_apellido STRING ,
    ds_rec_usuario_modif_nombre STRING ,
    ds_rec_usuario_modif_apellido STRING 
)
PARTITIONED BY (
    partition_date STRING )
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  '${DATA_LAKE_SERVER}/santander/bi-corp/common/reclamos/bt_rec_movimientos_tarjeta'