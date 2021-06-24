CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_common.bt_cla_altareset (

cod_cla_tipo_operacion      string,
cod_cla_usuario1            string,
cod_cla_usuario2            string,
cod_per_nup                 bigint,
ts_cla_fecha	            string,
cod_cla_retorno             string
)
PARTITIONED BY (PARTITION_DATE STRING)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/common/interacciones_canales/cla/fact/bt_cla_altareset'