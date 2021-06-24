CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.xbgc803(
xbgc803_apli_act string,
xbgc803_codi_act string,
xbgc803_subc_act string,
xbgc803_ccont_act string,
xbgc803_codi_altair string,
xbgc803_apli_actual string,
xbgc803_secuencia string,
xbgc803_es_cheque string,
xbgc803_ind_retencion string,
xbgc803_aplica_origen string,
xbgc803_cod_ind_tipo string,
xbgc803_centro_contab string,
xbgc803_aplic_no_ctas string,
xbgc803_oficina string,
xbgc803_ind_inter_suc string,
xbgc803_ind_tipo_plan string,
xbgc803_ind_cont_apaq string,
xbgc803_ind_impuesto_competi string,
xbgc803_ind_acred_sueldo string,
xbgc803_ind_aplica_impu string,
xbgc803_ind_paquete_competi string,
filler_1 string,
xbgc803_ind_verificado string,
xbgc803_cod_interfaz string,
xbgc803_ind_impuesto_brutos string,
xbgc803_autoriza_suc string,
filler_2 string
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
  '${DATA_LAKE_SERVER}/santander/bi-corp/staging/malbgc/dim/xbgc803'