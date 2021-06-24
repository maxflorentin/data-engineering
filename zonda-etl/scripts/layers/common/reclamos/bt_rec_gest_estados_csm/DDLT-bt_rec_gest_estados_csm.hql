CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_common.bt_rec_gest_estados_csm (
    cod_rec_gestion INT,
    cod_rec_orden_estado INT ,
    cod_rec_estado STRING ,
    ts_rec_estado TIMESTAMP ,
    cod_rec_actor string,
    cod_rec_usuario STRING ,
    ds_rec_comentario STRING ,
    cod_rec_usuario_alta STRING ,
    ts_rec_alta TIMESTAMP ,
    cod_rec_usuario_modif STRING ,
    ts_rec_modif TIMESTAMP ,
    ts_rec_baja TIMESTAMP ,
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
  '${DATA_LAKE_SERVER}/santander/bi-corp/common/reclamos/bt_rec_gest_estados_csm'