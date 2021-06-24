CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_common.stk_adm_empresas_deudas_bancarias_cliente (
    id_adm_fecha_deuda int,
    cod_adm_banco int,
    fc_adm_acc_cto_plz decimal(16, 2),
    fc_adm_acc_lgo_plz decimal(16, 2),
    fc_adm_aecc_cto_plz decimal(16, 2),
    fc_adm_aecc_lgo_plz decimal(16, 2),
    fc_adm_dto_doc_cto_plz decimal(16, 2),
    fc_adm_dto_doc_lgo_plz decimal(16, 2),
    fc_adm_leasing_cto_plz decimal(16, 2),
    fc_adm_leasing_lgo_plz decimal(16, 2),
    fc_adm_gtia_real_cto_plz decimal(16, 2),
    fc_adm_gtia_real_lgo_plz decimal(16, 2),
    fc_adm_otr_gtias_cto_plz decimal(16, 2),
    fc_adm_otr_gtias_lgo_plz decimal(16, 2),
    fc_adm_cart_cred_cto_plz decimal(16, 2),
    fc_adm_cart_cred_lgo_plz decimal(16, 2),
    fc_adm_avales_cto_plz decimal(16, 2),
    fc_adm_avales_lgo_plz decimal(16, 2),
    cod_adm_peusualt string,
    ts_adm_pefecalt string,
    cod_adm_peusumod string,
    ts_adm_pefecmod string,
    fc_adm_acc_cto_plz_dol decimal(16, 2),
    fc_adm_acc_lgo_plz_dol decimal(16, 2),
    fc_adm_aecc_cto_plz_dol decimal(16, 2),
    fc_adm_aecc_lgo_plz_dol decimal(16, 2),
    fc_adm_dto_doc_cto_plz_dol decimal(16, 2),
    fc_adm_dto_doc_lgo_plz_dol decimal(16, 2),
    fc_adm_leasing_cto_plz_dol decimal(16, 2),
    fc_adm_leasing_lgo_plz_dol decimal(16, 2),
    fc_adm_gtia_real_cto_plz_dol decimal(16, 2),
    fc_adm_gtia_real_lgo_plz_dol decimal(16, 2),
    fc_adm_otr_gtias_cto_plz_dol decimal(16, 2),
    fc_adm_otr_gtias_lgo_plz_dol decimal(16, 2),
    fc_adm_avales_cto_plz_dol decimal(16, 2),
    fc_adm_avales_lgo_plz_dol decimal(16, 2),
    cod_adm_col_variable_1 string,
    cod_adm_col_variable_2 string,
    fc_adm_variable_1_cto_plz decimal(16, 2),
    fc_adm_variable_1_lgo_plz decimal(16, 2),
    fc_adm_variable_1_cto_plz_dol decimal(16, 2),
    fc_adm_variable_1_lgo_plz_dol decimal(16, 2),
    fc_adm_variable_2_cto_plz decimal(16, 2),
    fc_adm_variable_2_lgo_plz decimal(16, 2),
    fc_adm_variable_2_cto_plz_dol decimal(16, 2),
    fc_adm_variable_2_lgo_plz_dol decimal(16, 2)
)
PARTITIONED BY (
  partition_date string)
STORED AS PARQUET
LOCATION
  '${DATA_LAKE_SERVER}/santander/bi-corp/common/admision/stk_adm_empresas_deudas_bancarias_cliente';



