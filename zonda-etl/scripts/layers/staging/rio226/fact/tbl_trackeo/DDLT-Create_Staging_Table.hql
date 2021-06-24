CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.rio226_tbl_trackeo
(
 INTERACCION                            string
,CODIGO_EVENTO                          string
,FECHA_EVENTO                           string
,INFORMACION_ADICIONAL                  string
,NRO_LLAMADO                            string
,ID_TRACKEO_TRANSACCION                 string

)
PARTITIONED BY(partition_date string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '${DATA_LAKE_SERVER}//santander/bi-corp/staging/rio226/fact/tbl_trackeo';
