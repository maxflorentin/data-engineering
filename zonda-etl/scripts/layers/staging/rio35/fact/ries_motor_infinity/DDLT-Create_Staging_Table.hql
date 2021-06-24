CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.rio35_ries_motor_infinity(
PENUMPER                string,
CODIGO_RECHAZO          string,
MOTIVO_RECHAZO          string,
APELLIDO                string,
NOMBRE                  string,
PRODUCTO_PAQ            string,
SUBPRODU_PAQ            string,
LINEA_PESOS             string,
LINEA_DOLARES           string,
PRES_PESOS_DISPONIBLE   string,
PRES_DOLARES_DISPONIBLE string,
CUOTA_PESOS             string,
CUOTA_DOLARES           string
)
PARTITIONED BY (PARTITION_DATE string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/staging/rio35/fact/ries_motor_infinity';