CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.rio4_cierrebna(
ESPECIE             STRING,
PRECIOCOMPRA        STRING,
PRECIOVENTA         STRING,
PRECIO1             STRING,
PRECIO2             STRING,
PRECIO3             STRING,
PRECIO4             STRING,
PRECIO5             STRING,
PORCADA             STRING,
MERCADO             STRING
)
PARTITIONED BY(FECHA string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/staging/rio4/fact/cierrebna';