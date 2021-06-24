create external table if not exists bi_corp_staging.afip(
cuit  string,
denominacion string,
imp_ganancias  string,
imp_iva  string,
monotributo  string,
integrante_soc  string,
empleador  string,
actividad_monotributo string
)
PARTITIONED BY (partition_date string)
STORED AS PARQUET
LOCATION
  '${DATA_LAKE_SERVER}/santander/bi-corp/staging/manual/adicional_juridica/afip';