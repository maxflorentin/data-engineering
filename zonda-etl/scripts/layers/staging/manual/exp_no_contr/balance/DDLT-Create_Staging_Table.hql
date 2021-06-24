CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.manual_balance(
         sociedad string ,
         cargabal string,
         sociedad_eliminacion string,
         importe string,
         cta_girada string,
         cod_moneda string
)
PARTITIONED BY (partition_date string)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  '${DATA_LAKE_SERVER}/santander/bi-corp/staging/manual/exp_no_contr/balance';