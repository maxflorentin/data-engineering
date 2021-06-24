CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.rio226_tbl_trackeo_evento
(
 CODIGO                         string
,DESCRIPCION                    string
,OPERACION                      string
,VISIBLE                        string
,DESCRIPCION_USUARIO            string



)
PARTITIONED BY(partition_date string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/staging/rio226/dim/tbl_trackeo_evento';
