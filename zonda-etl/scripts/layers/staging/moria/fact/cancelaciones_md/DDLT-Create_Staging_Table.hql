CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.moria_cancelaciones_md(
mdec160n_cod_entigen string,
mdec160n_idf_cancelac string,
mdec160n_fec_can_oper string,
mdec160n_fec_can_valo string,
mdec160n_cod_estcance string,
mdec160n_ind_tipcance string,
mdec160n_cod_tipcance string,
mdec160n_cod_canal string,
mdec160n_cod_centcanc string,
mdec160n_cod_oficcanc string,
mdec160n_cod_usrcanc string,
mdec160n_cod_diviorig string,
mdec160n_imp_totcanco decimal(17,4),
mdec160n_imp_capitalo decimal(17,4),
mdec160n_imp_intereso decimal(17,4),
mdec160n_imp_impueso decimal(17,4),
mdec160n_tip_cbiocanc decimal(17,4),
mdec160n_num_boleto string,
mdec160n_imp_totcanc decimal(17,4),
mdec160n_imp_capitalc decimal(17,4),
mdec160n_imp_interesc decimal(17,4),
mdec160n_imp_impuesc decimal(17,4),
mdec160n_cod_formpago string,
mdec160n_cod_divifpgo string,
mdec160n_cod_enticta string,
mdec160n_cod_centcta string,
mdec160n_num_cuenta string,
mdec160n_idf_nio_mir string,
mdec160n_idf_nio_dos string,
mdec160n_idf_nio_tres string,
mdec160n_num_persona string,
mdec160n_num_quita string,
mdec160n_num_proppago string,
mdec160n_num_pagoeejj string,
mdec160n_num_propcie string,
mdec160n_num_bufete string,
mdec160n_cod_entidadp string,
mdec160n_cod_centrop string,
mdec160n_num_pp string,
mdec160n_cod_gestor string
)
PARTITIONED BY (partition_date string)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  '${DATA_LAKE_SERVER}/santander/bi-corp/staging/moria/fact/moria_cancelaciones_md';