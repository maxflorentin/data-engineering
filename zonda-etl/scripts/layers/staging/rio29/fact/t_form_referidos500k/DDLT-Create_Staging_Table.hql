CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.rio29_t_form_referidos500k
(
id                      string,
rfte_nup                string,
rfte_nombre             string,
rfte_apellido           string,
rfte_nrodni             string,
rfte_afinidad           string,
rfdo_nombre             string,
rfdo_apellido           string,
rfdo_nrodni             string,
rfdo_cod_area_tel       string,
rfdo_nro_tel            string,
rfdo_email              string,
cod_rechazo             string,
tipo                    string,
premio                  string,
cod_campania            string,
cod_origen              string,
ip                      string,
fechaalta               string
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
  '${DATA_LAKE_SERVER}/santander/bi-corp/staging/rio29/fact/t_form_referidos500k/'