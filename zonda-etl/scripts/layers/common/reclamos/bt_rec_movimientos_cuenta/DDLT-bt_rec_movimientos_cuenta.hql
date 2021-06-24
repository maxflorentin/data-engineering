CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_common.bt_rec_movimientos_cuenta (
    cod_rec_gestion	INT ,
    cod_rec_nro_movimiento	INT ,
    cod_suc_sucursal_cuenta	INT ,
    cod_cue_cuenta	int ,
    cod_rec_divisa_cuenta	STRING ,
    cod_rec_nro_comprobante	INT ,
    cod_rec_cod_operativo	STRING ,
    ds_rec_operativo	STRING ,
    dt_rec_fecha_movimiento	STRING ,
    fc_rec_monto_movimiento	DECIMAL(15,2) ,
    cod_rec_ind_devolucion	STRING ,
    ts_rec_fecha_devolucion	TIMESTAMP ,
    fc_rec_monto_devolucion	DECIMAL(15,2) ,
    cod_rec_cod_rubro	INT ,
    cod_rec_nro_establecimiento	STRING ,
    cod_rec_usuario_alta	STRING ,
    ts_rec_fecha_alta	TIMESTAMP ,
    cod_rec_usuario_modif	STRING ,
    ts_rec_fecha_modif	TIMESTAMP ,
    ts_rec_fecha_baja	TIMESTAMP ,
    ds_rec_nombre_establecimiento	STRING ,
    cod_rec_cod_autorizacion	STRING ,
    cod_rec_nro_cupon	string ,
    ds_rec_nombre_empresa	STRING ,
    ds_rec_empresa_b24	STRING ,
    fc_rec_monto_reclamado	DECIMAL(15,2) ,
    dec_rec_tasa_promedio	DECIMAL(15,2) ,
    fc_rec_monto_interes	DECIMAL(15,2) ,
    fc_rec_monto_comision	DECIMAL(15,2) ,
    cod_rec_motivo	INT ,
    cod_rec_cod_op_brio_grupo	STRING ,
    cod_rec_cod_op_brio_subgrupo	STRING ,
    cod_rec_nro_tarjeta_debito	STRING ,
    cod_rec_atm	STRING ,
    ds_rec_usuario_alta_nombre	STRING ,
    ds_rec_usuario_alta_apellido	STRING ,
    ds_rec_usuario_modif_nombre	STRING ,
    ds_rec_usuario_modif_apellido	STRING 
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
  '${DATA_LAKE_SERVER}/santander/bi-corp/common/reclamos/bt_rec_movimientos_cuenta'