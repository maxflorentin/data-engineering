CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.malbgc_bgdtmco(
    mco_num_convenio	int,
    mco_convenio	string,
    mco_producto	string,
    mco_subprodu	string,
    mco_suscriptor	string,
    mco_entidad	string,
    mco_centro_alta	string,
    mco_cuenta	string,
    mco_divisa	string,
    mco_indesta	string,
    mco_fecha_est	string,
    mco_num_mes	int,
    mco_saldo_medio	decimal(15,2),
    mco_saldo_med_cv	decimal(17,4),
    mco_tipo_convenio	string,
    mco_num_asociados	int,
    mco_dias_vigencia	int,
    mco_entidad_umo	string,
    mco_centro_umo	string,
    mco_userid_umo	string,
    mco_netname_umo	string,
    mco_timest_umo	string
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
  '${DATA_LAKE_SERVER}/santander/bi-corp/staging/malbgc/fact/bgdtmco/consolidated';