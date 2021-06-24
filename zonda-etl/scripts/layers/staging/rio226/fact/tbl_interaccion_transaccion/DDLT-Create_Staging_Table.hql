CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.rio226_tbl_interaccion_transaccion
(
 CD_INTERACCION                         string
,ID_TRANSACCION                         string
,NUMERO_OPERACION                       string
,DATOS                                  string
,MARCA_REVERSADO                        string
,MARCA_OPERACION_EXITOSA                string
,NOMBRE_SERVICIO                        string
,ID_TRANSACCION_REVERSADO               string
,FECHA_MODIFICACION                     string
,TIPO_OPERACION                         string
,MARCA_REVERSABLE                       string
,ID_SERVICIO                            string
,ID_TRACKEO_TRANSACCION                 string


)
PARTITIONED BY(partition_date string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/staging/rio226/fact/tbl_interaccion_transaccion';
