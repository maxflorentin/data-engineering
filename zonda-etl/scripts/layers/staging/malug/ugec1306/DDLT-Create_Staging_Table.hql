CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.ugec1306
(
nup string ,
cuenta_prestamo string ,
prod string ,
sprod string ,
divisa string ,
fecha_vto string ,
fecha_cobro string,
imp_cobro string ,
cap_fact string ,
cap_rem_prev string ,
cap_cobrado string ,
int_fact string ,
int_rem_prev string ,
int_cobrado string ,
int_comp_calc string ,
int_comp_rem_prev string ,
int_comp_cobrado string ,
int_mor_calc string ,
int_mor_rem_prev string ,
int_mor_cobrado string ,
seg_fact string ,
seg_rem_prev string ,
seg_cobrado string ,
imp_fact string ,
imp_rem_prev string ,
imp_cobrado string ,
com_fact string ,
com_rem_prev string ,
com_cobrado string ,
gas_fact string ,
gas_rem_prev string ,
gas_cobrado string ,
cuenta_debito string ,
saldo_prev string ,
tipo_cobro string )
PARTITIONED BY (partition_date string)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  '${DATA_LAKE_SERVER}/santander/bi-corp/staging/malug/fact/ugec1306'