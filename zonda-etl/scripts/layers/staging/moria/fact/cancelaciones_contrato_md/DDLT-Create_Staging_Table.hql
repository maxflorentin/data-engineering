CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.moria_cancelaciones_contrato_md(
mdec160c_cod_entigen string,
mdec160c_idf_cancelac string,
mdec160c_cod_entidad string,
mdec160c_cod_centro string,
mdec160c_num_contrato string,
mdec160c_cod_producto string,
mdec160c_cod_subprodu string,
mdec160c_cod_divisa string,
mdec160c_num_posicion string,
mdec160c_num_ordenjud string,
mdec160c_cod_ordenjud string,
mdec160c_num_anoauto string,
mdec160c_num_juzgado string,
mdec160c_imp_canccto decimal(17,4),
mdec160c_imp_cancgasc decimal(17,4),
mdec160c_ind_pdteproc string,
mdec160c_fec_rendicio string,
mdec160c_cod_tipadmina string,
mdec160c_cod_tipadmin string,
mdec160c_sdo_intedoca decimal(17,4),
mdec160c_sdo_intedocu decimal(17,4)
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
  '${DATA_LAKE_SERVER}/santander/bi-corp/staging/moria/fact/moria_cancelaciones_contrato_md'