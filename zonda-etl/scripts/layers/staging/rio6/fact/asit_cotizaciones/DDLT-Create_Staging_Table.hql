CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.rio6_asit_cotizaciones(
    asco_coti_json       string,
    asco_nu_cotizacion   string,
    asco_fe_cotizacion   string,
    asco_tp_documento    string,
    asco_nu_documento    string,
    asco_in_enviada      string
)

PARTITIONED BY (partition_date string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/staging/rio6/fact/asit_cotizaciones';