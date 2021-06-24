CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_common.bt_rec_circ_info_reque (
  cod_rec_entidad string
  ,cod_rec_circuito int
  ,flag_rec_info_gpo string
  ,cod_rec_info_reque int
  ,flag_rec_info_oblig string
  ,cod_estado_circ_info_reque string
  ,int_rec_orden_info int
  ,cod_rec_etapa int
  ,int_rec_orden_etapa int
  ,ds_rec_info_reque string
  ,cod_rec_tipo_data int
  ,int_rec_long_info_reque int
  ,fc_rec_cant_dec_info_reque int
  ,int_rec_rang_dde_info_reque int
  ,int_rec_rang_hta_info_reque int
  ,dt_rec_dde_info_reque timestamp
  ,dt_rec_hta_info_reque timestamp
  ,cod_rec_estado_info_reque string
  ,cod_rec_tipo_campo int
  ,cod_rec_funcion_asco int
  ,cod_rec_info_relac int
  ,int_rec_tipo_info_especial int
  ,cod_rec_sector_owner string
  ,ds_rec_texto_ayuda string
  ,cod_rec_info_condic int
  ,cod_rec_valor_condic string
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
  '${DATA_LAKE_SERVER}/santander/bi-corp/common/reclamos/bt_rec_circ_info_reque'