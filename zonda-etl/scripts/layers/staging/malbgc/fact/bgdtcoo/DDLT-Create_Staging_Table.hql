CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.bgdtcoo(
coo_codigo string,
coo_alfa_chica string,
coo_alfa_grande string,
coo_cod_anulacion string,
coo_tipo_valor string,
coo_num_dias_asu  string,
coo_sentido_asu string,
coo_tipo_dias_asu string,
coo_nume_dias_inf string,
coo_sentido_inf string,
coo_tipo_dias_inf string,
coo_nume_dias_sup string,
coo_sentido_sup string,
coo_tipo_dias_sup string,
coo_ind_acti string,
coo_ind_obser string,
coo_ind_cheque string,
coo_ind_blq_cheque string,
coo_ind_tipo string,
coo_ind_apecan string,
coo_ind_anula string,
coo_ind_cobrar_impu string,
coo_ind_cobrar_com string,
coo_cod_interfaz string,
coo_cod_contab string,
coo_cod_clacon string,
coo_cod_concep_imp string,
coo_ind_apli_ext string,
coo_ind_sin_doc string,
coo_ind_si_plazo string,
coo_ind_giro_dep string,
coo_ind_grupo string,
coo_ind_lavado string,
coo_ind_cliente string,
coo_ind_sal_ext string,
coo_bcra_lavado string,
coo_bcra_agrupa string,
coo_cod_datanet string,
coo_ind_siter string,
coo_cod_aplic_origen string,
coo_tip_retencion string,
coo_centro_contab string,
coo_cod_aplic_no_ctas string,
coo_cod_destino_mov string,
coo_cod_tributa_ca string,
coo_cod_tributa_cc string,
coo_ind_acred_sueldo string,
coo_cls_mov string,
coo_entidad_umo string,
coo_centro_umo string,
coo_userid_umo string,
coo_netname_umo string,
coo_timest_umo string,
coo_cod_operacio_emf string,
coo_tip_info_embargos string,
coo_fec_ultimo_uso string,
coo_fec_penultimo_uso string,
coo_num_dias_sin_uso string,
coo_cod_alfa string,
coo_cod_num string,
coo_fec_aprob_req string
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
  '${DATA_LAKE_SERVER}/santander/bi-corp/staging/malbgc/dim/bgdtcoo'