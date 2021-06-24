CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_common.stk_cyp_comisiones(
cod_cyp_tab_pre                         INT,
cod_cyp_prod                            INT,
ds_cyp_tab_pre                          STRING,
fc_cyp_imp_comdoc                       DECIMAL(15,2),
fc_cyp_imp_commindoc                    DECIMAL(15,2),
fc_cyp_imp_commaxdoc                    DECIMAL(15,2),
fc_cyp_com_trn                          DECIMAL(15,2),
fc_cyp_com_mintrn                       DECIMAL(15,2),
fc_cyp_com_maxtrn                       DECIMAL(15,2),
fc_cyp_porc_com                         DECIMAL(9,5),
fc_cyp_imp_bltarecbas                   DECIMAL(15,2),
fc_cyp_porc_usocanldoc                  DECIMAL(9,5),
fc_cyp_imo_usocanldoc                   DECIMAL(15,2),
fc_cyp_porc_usoadic                     DECIMAL(9,5),
fc_cyp_imp_comusoadic                   DECIMAL(15,2),
fc_cyp_porc_canltrn                     DECIMAL(9,5),
fc_cyp_imp_usocanltrn                   DECIMAL(15,2),
fc_cyp_imp_comnofinan                   DECIMAL(15,2),
fc_cyp_imp_comdoc3ra                    DECIMAL(15,2),
fc_cyp_imp_mindoc3ra                    DECIMAL(15,2),
fc_cyp_imp_maxdoc3ra                    DECIMAL(15,2),
fc_cyp_com_trn3ra                       DECIMAL(15,2),
fc_cyp_mintrn3ra                        DECIMAL(15,2),
fc_cyp_maxtrn3ra                        DECIMAL(15,2),
fc_cyp_porc_3ra                         DECIMAL(9,5),
fc_cyp_imp_recarnoclte                  DECIMAL(15,2),
fc_cyp_porc_recarnoclte                 DECIMAL(9,5),
fc_cyp_porc_canldoc3ra                  DECIMAL(9,5),
fc_cyp_imp_canldoc3ra                   DECIMAL(15,2),
fc_cyp_porc_usoadic3ra                  DECIMAL(9,5),
fc_cyp_imp_usoadic3ra                   DECIMAL(15,2),
fc_cyp_porc_canltrn3ra                  DECIMAL(9,5),
fc_cyp_imp_canl_trn3ra                  DECIMAL(15,2),
fc_cyp_imp_nofinan3ra                   DECIMAL(15,2)
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
  '${DATA_LAKE_SERVER}/santander/bi-corp/common/cyp/fact/stk_cyp_comisiones'