CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.fm_rescates
(
 cod_fdo string,
 cod_cliente string,
 certificado string,
 nr_rgt string,
 cod_fdo_dest string,
 cod_cli_dest string,
 nr_cert_dest string,
 cod_agte_solic string,
 cod_canal_solic string,
 cod_agte_dg string,
 cod_can_dg string,
 cod_oper_dg string,
 cod_agte_cd string,
 cod_can_cd string,
 cod_oper_cd string,
 dtinput string,
 hora string,
 dtsolic string,
 dtconver string,
 dtpagto string,
 dtestorno string,
 forma_pagto string,
 tipo_cta string,
 moeda_cta string,
 nr_cta_tip string,
 nr_cta_num string,
 perc_comis string,
 val_rgt_brt string,
 val_rgt_liq string,
 val_cust_rgt string,
 qt_cot_rgt string,
 val_cota_rgt string,
 num_cancel string,
 moeda_cambio string,
 cotacao_indi string,
 vl_conv_pact string,
 cod_can_liq string,
 nr_oper string,
 liqu_rgt string,
 cliente_ac string
)
PARTITIONED BY (partition_date string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/staging/fm/fact/rescates'