CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.rio29_t_form_select_eventos
(
nombre             string,
apellido           string,
cod_area_celular   string,
nro_celular        string,
email              string,
eventos_participar string,
localidad          string,
fechaalta          string,
ip                 string,
tipodni            string,
nrodni             string
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
  '${DATA_LAKE_SERVER}/santander/bi-corp/staging/rio29/fact/t_form_select_eventos/'