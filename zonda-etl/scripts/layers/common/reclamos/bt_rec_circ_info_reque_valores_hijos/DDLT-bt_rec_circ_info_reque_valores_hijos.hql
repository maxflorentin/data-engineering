CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_common.bt_rec_circ_info_reque_valores_hijos (
  cod_rec_entidad string
  ,cod_rec_info_reque_p int
  ,cod_rec_valor_p string
  ,cod_rec_nro_valor_p int
  ,cod_rec_info_reque_h int
  ,cod_rec_valor_h string
  ,cod_rec_nro_valor_h int
  ,ds_rec_desc_valor string
  ,cod_rec_estado_valor string
  ,cod_rec_valor_info_h string
  ,cod_rec_valor_info_p string
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
  '${DATA_LAKE_SERVER}/santander/bi-corp/common/reclamos/bt_rec_circ_info_reque_valores_hijos'