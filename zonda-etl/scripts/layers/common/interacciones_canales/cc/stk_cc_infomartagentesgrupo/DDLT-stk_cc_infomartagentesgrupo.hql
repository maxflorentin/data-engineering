CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_common.stk_cc_infomartagentesgrupo (

cod_cc_legajo       string,
ds_cc_nombre        string,
ds_cc_apellido      string,
ds_cc_codigo_login  string,
ds_cc_grupo         string
)
PARTITIONED BY (
    partition_date string)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
'${DATA_LAKE_SERVER}/santander/bi-corp/common/interacciones_canales/cc/fact/stk_cc_infomartagentesgrupo'
