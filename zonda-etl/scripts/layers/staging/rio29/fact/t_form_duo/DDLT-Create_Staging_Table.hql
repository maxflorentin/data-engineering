CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.rio29_t_form_duo
(
id                string,
nombre            string,
dni               string,
telefono          string,
email             string,
fecha_alta        string,
provincia         string,
es_cliente        string

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
  '${DATA_LAKE_SERVER}/santander/bi-corp/staging/rio29/fact/t_form_duo/'