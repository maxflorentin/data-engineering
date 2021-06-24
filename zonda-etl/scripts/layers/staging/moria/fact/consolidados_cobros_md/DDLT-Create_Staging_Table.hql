CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.moria_consolidados_cobros_md(
mdec160r_cod_entigen string,
mdec160r_idf_cancelac string,
mdec160r_cod_entidad string,
mdec160r_cod_centro string,
mdec160r_num_contrato string,
mdec160r_cod_producto string,
mdec160r_cod_subprodu string,
mdec160r_cod_divisa string,
mdec160r_num_recibo string,
mdec160r_imp_cancreci string,
mdec160r_imp_condreci string,
mdec160r_cod_estrecia string,
mdec160r_cod_estreci string,
mdec160r_ind_fac_onln string,
mdec160r_fec_ultpgina string,
mdec160r_fec_ultpgint string,
mdec160r_cod_coefinde string,
mdec160r_fac_indexa string,
mdec160r_fec_ultindxa string,
mdec160r_fec_ultindex string,
mdec160r_int_tcpsteor string,
mdec160r_por_morteor string,
mdec160r_int_tcpsapli string,
mdec160r_por_morapli string,
mdec160r_por_ajusapli string,
mdec160r_total_contable string,
mdec160r_total_no_contable string,
mdec160r_impdev_capital string,
mdec160r_impdev_interes string,
mdec160r_impdev_ajuste string,
mdec160r_impdev_ajuste_1 string,
mdec160r_impdev_ajuste_signo string,
mdec160r_impudev_total string,
mdec160r_impudev_iva1 string,
mdec160r_impudev_iva2 string,
mdec160r_impudev_ing_b string,
mdec160r_impudev_imp_e string,
mdec160r_impudev_otro string,
mdec160r_imperc_comision string,
mdec160r_imperc_seg_vida string,
mdec160r_imperc_seg_ince string,
mdec160r_imperc_seg_total string,
mdec160r_imperc_gastos_md_ug string,
mdec160r_imperc_int_cps string,
mdec160r_imperc_int_mor string,
mdec160r_imperc_ajuste string,
mdec160r_imperc_interes string,
mdec160r_impuperc_total string,
mdec160r_impuperc_iva1 string,
mdec160r_impuperc_iva2 string,
mdec160r_impuperc_ing_b string,
mdec160r_impuperc_imp_e string,
mdec160r_impuperc_otro string
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
  '${DATA_LAKE_SERVER}/santander/bi-corp/staging/moria/fact/moria_consolidados_cobros_md'