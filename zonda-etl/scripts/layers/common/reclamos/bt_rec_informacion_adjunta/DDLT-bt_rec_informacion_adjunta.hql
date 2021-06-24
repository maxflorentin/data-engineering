CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_common.bt_rec_informacion_adjunta (
    cod_rec_entidad string
    , cod_rec_sector string
    , cod_rec_gestion_nro int
    , cod_rec_actor int
    , int_rec_actor int
    , cod_rec_tipo_info int
    , cod_rec_info_reque int
    , ds_rec_dato_info_reque string
    , ds_rec_link_inf_adjunta string
    , ts_rec_info_adjunta TIMESTAMP
    , ds_rec_usuario_info_adjunta string
    , cod_rec_sector_info_adjunta string
    , ds_rec_nombre_archivo_original string
    , ds_rec_nombre_archivo string
    , ds_rec_nombre_directorio string
    , int_rec_tamano_inf_adj int
    , int_rec_secuencia_inf_adj int
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
  '${DATA_LAKE_SERVER}/santander/bi-corp/common/reclamos/bt_rec_informacion_adjunta'