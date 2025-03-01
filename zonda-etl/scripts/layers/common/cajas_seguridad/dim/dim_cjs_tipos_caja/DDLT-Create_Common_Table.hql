CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_common.dim_cjs_tipos_caja(
                cod_cjs_tipo_caja BIGINT
               ,cod_cjs_grupo_caja BIGINT
               ,ds_cjs_grupo_caja string
               ,cod_cjs_medida_caja string)
PARTITIONED BY (
    partition_date string)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  '${DATA_LAKE_SERVER}/santander/bi-corp/common/cajas_seguridad/dim/dim_cjs_tipos_caja';