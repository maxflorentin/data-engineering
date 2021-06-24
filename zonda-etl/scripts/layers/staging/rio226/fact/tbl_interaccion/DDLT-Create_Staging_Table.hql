CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.rio226_tbl_interaccion
(
 CD_INTERACCION               string
,NUP                          string
,CD_EJECUTIVO                 string
,DT_INICIO                    string
,DT_CIERRE                    string
,CD_CANAL_COMUNICACION        string
,CD_CANAL_VENTA               string
,CD_SUCURSAL                  string
,DS_COMENTARIO                string
,MOTIVO                       string
,NRO_LLAMADO                  string
,ORIGEN                       string
,CD_INTERACCION_PADRE         string
,NUMERO_TICKET                string

)
PARTITIONED BY(partition_date string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/staging/rio226/fact/tbl_interaccion';
