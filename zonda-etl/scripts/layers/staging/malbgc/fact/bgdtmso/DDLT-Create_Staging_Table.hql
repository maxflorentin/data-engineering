CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.bgdtmso(
  bgtcmso_mso_entidad string,
  bgtcmso_mso_centro_alta string,
  bgtcmso_mso_cuenta string,
  bgtcmso_mso_divisa string,
  bgtcmso_mso_codigo string,
  bgtcmso_mso_fecha_oper string,
  bgtcmso_mso_fecha_vtovig string,
  bgtcmso_mso_importe_cuota decimal(17,4),
  bgtcmso_mso_importe_sobregirado decimal(17,4),
  bgtcmso_mso_dispo_aut decimal(17,4),
  bgtcmso_mso_dispo_des decimal(17,4),
  bgtcmso_mso_disp_total_acu decimal(17,4),
  bgtcmso_mso_observa string,
  bgtcmso_mso_entidad_umo string,
  bgtcmso_mso_centro_umo string,
  bgtcmso_mso_userid_umo string,
  bgtcmso_mso_netname_umo string,
  bgtcmso_mso_timest_umo string)
PARTITIONED BY (
  partition_date string)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  '${DATA_LAKE_SERVER}/santander/bi-corp/staging/malbgc/fact/bgdtmso';