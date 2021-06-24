CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.malug_ugdtaux
(
  entidad string,
  oficina string,
  cuenta string,
  pais_re string,
  int_document string,
  int_document_sal string,
  tipogar_index_ini string,
  coefici_visual string,
  feccal_index_ini string,
  valcoef1_index_ini string,
  valcoef2_index_ini string,
  factor_index_ini string,
  feccal_index_ult string,
  valcoef1_index_ult string,
  valcoef2_index_ult string,
  factor_index_ult string,
  impajust_sal_ult string,
  impajust_sal_acum string,
  codconli_ajusal string,
  coefici_index_act string,
  saldo_ajuste string,
  entidad_umo string,
  centro_umo string,
  userid_umo string,
  netname_umo string,
  timest_umo string,
  por_cft_ori string,
  por_cft_act string,
  ind_mipyme string,
  cftna_simp_ori string,
  cftna_simp_act string
)
PARTITIONED BY (partition_date string)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  '${DATA_LAKE_SERVER}/santander/bi-corp/staging/malug/fact/ugdtaux'
