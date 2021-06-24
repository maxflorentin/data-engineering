CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.rio29_t_form_work_cafe
(
nombre      string,
nrodni      string,
email       string,
nro_celular string,
motivo      string,
sucursal    string,
fecha_op1   string,
horario_op1 string,
fecha_op2   string,
horario_op2 string,
ip          string,
fechaalta   string,
id          string
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
  '${DATA_LAKE_SERVER}/santander/bi-corp/staging/rio29/fact/t_form_work_cafe/'