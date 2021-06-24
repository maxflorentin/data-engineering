CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_common.stk_cla_marcapin (

cod_per_nup             bigint,
cod_cla_tipoclave       string,
dt_cla_fecha_alta       string,
dt_cla_fecha_cambio     string,
dt_cla_fecha_acceso     string,
flag_cla_marca_bloqueo	int
)
PARTITIONED BY (PARTITION_DATE STRING)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/common/interacciones_canales/cla/fact/stk_cla_marcapin'