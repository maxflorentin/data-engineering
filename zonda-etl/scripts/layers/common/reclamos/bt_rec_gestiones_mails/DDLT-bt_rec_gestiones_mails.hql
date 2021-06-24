CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_common.bt_rec_gestiones_mails (
    cod_rec_gestion INT,
    cod_rec_orden_estado STRING,
    ds_rec_email STRING,
    ds_rec_asunto_mail STRING,
    ds_rec_texto_mail STRING,
    cod_rec_estado_mail STRING,
    ts_rec_alta TIMESTAMP,
    ts_rec_envio TIMESTAMP,
    ds_rec_tipo_mail STRING,
    cod_rec_usuario_alta STRING,
    cod_rec_usuario_modif STRING,
    ts_rec_modif TIMESTAMP,
    ts_rec_baja TIMESTAMP,
    cod_rec_nro_envio INT,
    flag_rec_automatico INT,
    ds_rec_usuario_alta_nombre STRING,
    ds_rec_usuario_alta_apellido STRING,
    ds_rec_usuario_modif_nombre STRING,
    ds_rec_usuario_modif_apellido STRING )
PARTITIONED BY (
    partition_date STRING)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  '${DATA_LAKE_SERVER}/santander/bi-corp/common/reclamos/bt_rec_gestiones_mails';