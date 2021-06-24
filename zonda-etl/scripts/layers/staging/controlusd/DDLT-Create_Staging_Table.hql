CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.control_usd (
    sucursal string,
    cuenta string,
    cod_op string,
    importe string,
    extraction_date string
)
PARTITIONED BY(partition_date string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/staging/malbgc/fact/control_usd';