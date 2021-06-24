CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.relacion_contrato_cru(

  num_persona string,
  nro_propuesta string,
  cod_entidad string,
  cod_centro string,
  num_contrato string,
  cod_producto string,
  cod_subprodu string,
  cod_divisa string,
  cod_entidad_rel string,
  cod_centro_rel string,
  num_contrato_rel string,
  cod_producto_rel string,
  cod_subprodu_rel string,
  cod_divisa_rel string,
  imp_saldo_cierre string,
  porc_deuda string,
  cod_tipo_rel string

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
  '${DATA_LAKE_SERVER}/santander/bi-corp/staging/moria/fact/relacion_contrato_cru'
