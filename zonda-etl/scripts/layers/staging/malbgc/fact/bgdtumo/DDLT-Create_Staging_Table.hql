CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.malbgc_bgdtumo(
umo_entidad string,
umo_centro_alta string,
umo_cuenta  string,
umo_divisa  string,
umo_numer_mov int,
umo_codigo  string,
umo_importe decimal(15,2),
umo_concepto string,
umo_base_impuesto decimal(15,2),
umo_cod_impuesto string,
umo_impor_impuesto decimal(15,2),
umo_cpto_impuesto string,
umo_fecha_conta string,
umo_fecha_oper string,
umo_hora_oper string,
umo_fecha_valor string,
umo_cheque  int,
umo_num_serie int,
umo_minibanco string,
umo_ref_interna string,
umo_ind_desdobla string,
umo_ind_observa string,
umo_saldo_aut decimal(15,2),
umo_entidad_cont string,
umo_centro_cont string,
umo_producto string,
umo_subprodu string,
umo_ind_ficticio string,
umo_ind_aom string,
umo_ind_act string,
umo_ind_anulable string,
umo_ind_anul string,
umo_ind_car_abo string,
umo_ind_giro_dep string,
umo_codigo_ur string,
umo_tipo_cambio decimal(9,6),
umo_canal   string,
umo_ind_contab string,
umo_ind_impto_mir string,
umo_ind_ppal string,
umo_transaccion string,
umo_cod_aplicacion string,
umo_term_nio string,
umo_fecha_nio string,
umo_hora_nio string,
umo_cod_oper_ppal string,
umo_cod_destino_mov string,
umo_cod_tributa string,
umo_ind_acred_sueldo string,
umo_numer_ret int,
umo_cod_aplic_contab string,
umo_centro_origen string,
umo_userid_aut string,
umo_ind_mov_genuino string,
umo_entidad_umo string,
umo_centro_umo string,
umo_userid_umo string,
umo_cajero_umo string,
umo_netname_umo string,
umo_timest_umo string,
umo_discriminante string,
umo_div_contraria string
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
  '${DATA_LAKE_SERVER}/santander/bi-corp/staging/malbgc/fact/bgdtumo'