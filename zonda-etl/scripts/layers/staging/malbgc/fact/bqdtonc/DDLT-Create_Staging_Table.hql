CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.bqdtonc(
onc_entidad string,
onc_centro_alta string,
onc_cuenta string,
onc_num_cheque  string,
onc_secuencia string,
onc_cont_onp string,
onc_cod_periodico  string,
onc_motivo string,
onc_observaciones  string,
onc_denunciante string,
onc_nombre_benef string,
onc_tipo_id_denun string,
onc_id_denun string,
onc_fecha_alta string,
onc_hora_alta string,
onc_usuario_alta string,
onc_importe string,
onc_divisa string,
onc_estado string,
onc_tipo_id_baja string,
onc_id_baja string,
onc_fecha_baja string,
onc_hora_baja string,
onc_usuario_baja string,
onc_tipo_id_conf string,
onc_id_conf string,
onc_fecha_confirm string,
onc_hora_confirm string,
onc_usuario_confir string,
onc_entidad_umo string,
onc_centro_umo string,
onc_userid_umo string,
onc_netname_umo string,
onc_timest_umo string,
onc_entidad_ogl string,
onc_centro_ogl string,
onc_cuenta_ogl string,
onc_divisa_ogl string
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
  '${DATA_LAKE_SERVER}/santander/bi-corp/staging/malbgc/fact/bqdtonc'