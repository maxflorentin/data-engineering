CREATE EXTERNAL TABLE IF NOT EXISTS BI_CORP_COMMON.DIM_OBE_TRANSACCIONES (

cod_obe_transaccion             string,
ds_obe_transaccion              string,
cod_obe_modulo                  string,
ds_obe_modulo                   string,
cod_obe_tipo_transaccion         string,
ds_obe_tipo_transaccion         string,
cod_obe_submodulo               string,
ds_obe_submodulo                string
)
PARTITIONED BY (PARTITION_DATE STRING)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/common/interacciones_canales/obe/dim/dim_obe_transacciones'