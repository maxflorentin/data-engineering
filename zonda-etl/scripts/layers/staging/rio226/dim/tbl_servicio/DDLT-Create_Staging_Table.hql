CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.rio226_tbl_servicio
(
 ID                         string
,NOMBRE                     string
,DESCRIPCION                string
,TIPO                       string
,MARCA_REVERSABLE           string
,MARCA_VENTA                string
,FECHA_MODIFICACION         string
,GRUPO                      string


)
PARTITIONED BY(partition_date string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/staging/rio226/dim/tbl_servicio';
