CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_common.stk_rcp_ventamoria (
  cod_rcp_venta int,
  cod_rcp_fedeicomiso int,
  cod_rcp_preventa int,
  cod_rcp_cotizacion int,
  dt_rcp_fechacorte string,
  cod_rcp_ip string,
  cod_rcp_usuario string,
  ds_rcp_estado string,
  dt_rcp_fechapreventa string,
  fc_rcp_cantcliagregados int,
  fc_rcp_cantcliexcluidos int,
  dt_rcp_fechaventa string,
  ds_rcp_usuarioalta string,
  ts_rcp_alta timestamp,
  ds_rcp_usuarioenvio string,
  ts_rcp_envio timestamp,
  ds_rcp_usuarioaprobacion string,
  ts_rcp_aprobacion timestamp,
  ds_rcp_motivorechazo string,
  dt_rcp_fechaajuste string,
  dt_rcp_fechaajuste2 string )
PARTITIONED BY(partition_date string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/common/recuperaciones/stk_rcp_ventamoria' ;