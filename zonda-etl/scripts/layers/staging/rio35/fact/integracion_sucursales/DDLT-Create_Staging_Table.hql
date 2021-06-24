CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.rio35_integracion_sucursales(
SUC_ANT            string,
SUC_NVA            string,
FECHA              string,
CAJA               string,
NOMBRE_SUC         string,
DIRE_SUC           string,
NSUC_ANT           string,
F_ACSE             string,
RESTO              string,
SUC_DEFAULT        string
)
PARTITIONED BY (PARTITION_DATE string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/staging/rio35/fact/integracion_sucursales';