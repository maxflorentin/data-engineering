CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.datos_gob_emae_indicadores(
        fecha string,
        original string,
        desestacionalizada string,
        tendencia_ciclo string,
        original_via string,
        desestacionalizada_vm string,
        tendencia_ciclo_vm string
)
PARTITIONED BY (partition_date string)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  '${DATA_LAKE_SERVER}/santander/bi-corp/staging/datos_gob/emae_indicadores';