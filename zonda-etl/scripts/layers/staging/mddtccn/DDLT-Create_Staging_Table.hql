CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.mddtccn(
  COD_ENTIGEN string,
  COD_ENTIDAD string,
  COD_CENTRO  string,
  NUM_CONTRATO string,
  COD_PRODUCTO string,
  COD_SUBPRODU string,
  COD_DIVISA  string,
  COD_ENTIDADG string,
  COD_CENTROG string,
  NUM_CONTRATG string,
  COD_PRODUCTG string,
  COD_SUBPRODG string,
  COD_DIVISAG string,
  FEC_BAJA    string,
  COD_USUALTA string,
  FEC_ALTAREG string,
  ENTIDAD_ALTA string,
  CENTRO_ALTA string,
  ENTIDAD_UMO string,
  CENTRO_UMO  string,
  USERID_UMO  string,
  NETNAME_UMO string,
  TIMEST_UMO  string)
PARTITIONED BY(partition_date string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/staging/moria/fact/mddtccn';