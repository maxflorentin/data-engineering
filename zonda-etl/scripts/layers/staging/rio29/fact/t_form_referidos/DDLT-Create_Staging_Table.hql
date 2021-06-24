CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.rio29_t_form_referidos
(
id          string,
nombre      string,
nrodni      string,
genero      string,
email       string,
nro_celular string,
legajo      string,
ip          string,
fecha       string,
programa    string
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
  '${DATA_LAKE_SERVER}/santander/bi-corp/staging/rio29/fact/t_form_referidos/'