CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.rio44_ba_equipos_dispo_tas_cerrado (
    fecha string,
    sigla string,
    posicion_num string,
    diponibilidad string,
    cash_out string,
    balanceo string,
    reciclador string )
PARTITIONED BY (partition_date string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/staging/rio44/fact/ba_equipos_dispo_tas_cerrado';