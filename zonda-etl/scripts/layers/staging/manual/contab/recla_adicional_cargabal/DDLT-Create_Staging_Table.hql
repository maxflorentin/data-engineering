create external table if not exists bi_corp_staging.recla_adicional_cargabal(
periodo string,
nup  string,
entidad  string,
sucursal  string,
cuenta  string,
producto  string,
subproducto  string,
cargabal_actual  string,
cargabal_nuevo  string
)
PARTITIONED BY (partition_date string)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  '${DATA_LAKE_SERVER}/santander/bi-corp/staging/manual/contab/recla_adicional_cargabal';