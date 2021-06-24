CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.orde_operativo_mvw_trk_pacha
(
LEGAJO string,
SUCURSAL string,
ID_EVENTO string,
FECHA string,
NUMERO_CUENTA string,
NUMERO_CHEQUE string,
MOTIVO string,
TIPO_CAMARA string,
ESTADO_INICIAL string,
ESTADO_SIGUIENTE string,
INFORMACION_ADICIONAL string
)
PARTITIONED BY(partition_date string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/staging/ordenador_operativo/fact/mvw_trk_pacha';
