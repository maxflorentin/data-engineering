CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.comex_rio39_swift_in(
ID_SWIFT                 STRING,
BENEF                    STRING,
MONEDA                   STRING,
MONTO                    STRING,
ESTADO                   STRING,
NRO_OP                   STRING,
USUARIO                  STRING,
FMODIF                   STRING,
REFERENCIA               STRING,
NRO_OPR                  STRING,
NROSWIFT                 STRING,
BANCO                    STRING,
FVALOR                   STRING,
TIPO_INGRESO             STRING,
GPI                      STRING,
SRP                      STRING,
CUENTA                   STRING,
CON_FONDOS               STRING,
FINGRESO_FONDOS          STRING,
FECHA_FONDOS             STRING,
FINGRESO                 STRING
)
PARTITIONED BY (PARTITION_DATE STRING)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/staging/rio39/fact/swift_in';