CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.rio29_t_form_cooperativa
(
id                         string,
nombre_apellido_rte        string,
nup_referente              string,
cod_area_tel_rte           string,
numero_tel_rte             string,
email_referente            string,
nombre_apellido_rdo        string,
dni_referido               string,
cod_area_tel_rdo           string,
numero_tel_rdo             string,
email_referido             string,
fecha                      string
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
  '${DATA_LAKE_SERVER}/santander/bi-corp/staging/rio29/fact/t_form_cooperativa/'