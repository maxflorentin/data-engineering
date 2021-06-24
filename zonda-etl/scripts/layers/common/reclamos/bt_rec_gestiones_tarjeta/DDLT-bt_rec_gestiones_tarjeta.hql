CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_common.bt_rec_gestiones_tarjeta (
    cod_rec_gestion INT ,
    cod_rec_orden_estado INT ,
    cod_rec_producto_altair STRING ,
    cod_suc_sucursal_cuenta INT ,
    cod_cue_cuenta INT ,
    cod_rec_estado_cuenta INT ,
    ts_rec_alta_cuenta TIMESTAMP ,
    cod_tcr_tarjeta INT ,
    fc_rec_meta_consumo_anual DECIMAL(15,2) ,
    fc_rec_consumo_acum_periodo_act DECIMAL(15,2) ,
    cod_rec_titular STRING ,
    cod_rec_producto_amas STRING ,
    cod_rec_digito_verificador INT ,
    cod_rec_prod_paquete_cuenta INT ,
    cod_rec_subprod_paquete_cuenta INT ,
    cod_rec_nro_cartera INT ,
    fc_rec_monto_saldo_acreedor DECIMAL(15,2) ,
    cod_rec_dev_acreedor STRING ,
    ts_rec_dev_acreedor TIMESTAMP ,
    fc_rec_monto_dev_acreedor DECIMAL(15,2) ,
    cod_rec_asdo STRING ,
    fc_rec_pago_minimo DECIMAL(15,2) ,
    fc_rec_monto_maximo_dev DECIMAL(15,2) ,
    fc_rec_monto_parcial_dev DECIMAL(15,2) ,
    cod_rec_motivo_baja_cta STRING ,
    fc_rec_monto_dev_acreedor_usd DECIMAL(15,2) ,
    fc_rec_monto_saldo_acreedor_usd DECIMAL(15,2) ,
    ts_rec_dev_acreedor_usd TIMESTAMP ,
    cod_rec_dev_acreedor_usd STRING ,
    cod_rec_usuario_alta STRING ,
    ts_rec_alta TIMESTAMP ,
    cod_rec_usuario_modif STRING ,
    ts_rec_modif TIMESTAMP ,
    ts_rec_baja TIMESTAMP ,
    ts_rec_ultimo_cierre TIMESTAMP ,
    flag_rec_trj_denunciante INT ,
    flag_rec_chip INT ,
    cod_per_nup_titular_tarjeta INT ,
    flag_rec_contacto_comercio INT ,
    ds_rec_usuario_alta_nombre STRING ,
    ds_rec_usuario_alta_apellido STRING ,
    ds_rec_usuario_modif_nombre STRING ,
    ds_rec_usuario_modif_apellido STRING 
)
PARTITIONED BY (
    partition_date STRING)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  '${DATA_LAKE_SERVER}/santander/bi-corp/common/reclamos/bt_rec_gestiones_tarjeta'