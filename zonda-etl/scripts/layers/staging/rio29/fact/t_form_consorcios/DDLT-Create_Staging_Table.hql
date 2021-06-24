CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.rio29_t_form_consorcios
(
id                        string,
razon_social              string,
cuit_consorcio            string,
admin_consorcio           string,
codarea_tel               string,
dni                       string,
email                     string,
es_admin_consorcio        string,
motivo_iniciativa         string,
numero_tel                string,
sucursal                  string,
cod_area_no_admin         string,
dni_no_admin              string,
email_no_admin            string,
nom_app_no_admin          string,
tel_no_admin              string,
fechaalta                 string
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
  '${DATA_LAKE_SERVER}/santander/bi-corp/staging/rio29/fact/t_form_consorcios/'