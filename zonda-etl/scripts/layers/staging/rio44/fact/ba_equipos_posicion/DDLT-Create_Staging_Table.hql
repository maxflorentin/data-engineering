CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.rio44_ba_equipos_posicion (
    id_equipos string,
    id_posicion string,
    f_alta string,
    f_baja string)
PARTITIONED BY (partition_date string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/staging/rio44/fact/ba_equipos_posicion';