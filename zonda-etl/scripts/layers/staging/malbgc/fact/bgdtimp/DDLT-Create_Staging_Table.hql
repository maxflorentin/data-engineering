CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.malbgc_bgdtimp(
  imp_entidad STRING,
  imp_centro_alta STRING,
  imp_cuenta STRING,
  imp_divisa STRING,
  imp_secuimp INT,
  imp_secuimp_adici INT,
  imp_concepto STRING,
  imp_fecha_imp STRING,
  imp_prioridad STRING,
  imp_importe_impago DECIMAL(17,4),
  imp_codoper_impago STRING,
  imp_base_impuesto DECIMAL(15,2),
  imp_impuesto DECIMAL(17,4),
  imp_codope_impuest STRING,
  imp_cpto_impuesto STRING,
  imp_ind_dec_div STRING,
  imp_fecha_cobro STRING,
  imp_import_totcob DECIMAL(17,4),
  imp_impago_cobro DECIMAL(17,4),
  imp_impu_cobro DECIMAL(17,4),
  imp_estado STRING,
  imp_cod_oper_ppal STRING,
  imp_entidad_umo STRING,
  imp_centro_umo STRING,
  imp_userid_umo STRING,
  imp_netname_umo STRING,
  imp_timest_umo STRING
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
  '${DATA_LAKE_SERVER}/santander/bi-corp/staging/malbgc/fact/bgdtimp/consolidated';