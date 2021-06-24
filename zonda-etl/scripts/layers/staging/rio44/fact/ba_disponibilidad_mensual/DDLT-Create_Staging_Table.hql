CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.rio44_ba_disponibilidad_mensual (
    fecha string,
    atm string,
    en_servicio string,
    sin_servicio string,
    hard string,
    suministros string,
    cash_out string,
    comunicacion string,
    host_down string,
    balanceo string,
    mant_prevent string,
    vandalismo string,
    logistica string,
    corte_energia string,
    dispensador string,
    reciclador string,
    deposito string,
    cheques string )
PARTITIONED BY (partition_date string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/staging/rio44/fact/ba_disponibilidad_mensual';