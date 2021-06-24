CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_common.stk_rcp_discrepanciasasdf (
  int_rcp_ejecucion int,
  dt_rcp_periodo int,
  cod_per_nup int,
  ds_per_numdoc bigint,
  ds_rcp_razonsocial string,
  ds_rcp_clasificacionedfsindiscrepancia string,
  ds_rcp_clasificaciondiscrepancia string,
  ds_clasificacionedf string,
  fc_rcp_deudatotal decimal(17,4),
  fc_rcp_previsiontotalsindiscrepancia decimal(17,4),
  fc_previsiontotal decimal(17,4)
)
PARTITIONED BY(partition_date string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/common/recuperaciones/stk_rcp_discrepanciasasdf' ;