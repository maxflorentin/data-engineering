CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.rio143_alta_reset_hist(
        tipo_operacion string,
        usuario1 string,
        usuario2 string,
        nup string,
        fecha_modificacion string,
        fecha_hora_modificacion string,
        cod_retorno string
)

PARTITIONED BY (partition_date string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/staging/rio143/fact/alta_reset';