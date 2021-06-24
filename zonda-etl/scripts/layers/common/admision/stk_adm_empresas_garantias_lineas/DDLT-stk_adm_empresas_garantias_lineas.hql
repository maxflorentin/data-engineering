CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_common.stk_adm_empresas_garantias_lineas (
    cod_adm_nro_prop bigint,
    cod_adm_operacion string,
    cod_adm_seq_lin_garan bigint,
    cod_adm_gartia string,
    int_adm_porc_garantia int,
    fc_adm_monto_garantia decimal(16, 2),
    ds_adm_detalle_garantia string,
    cod_adm_peusualt string,
    dt_adm_pefecalt string,
    cod_adm_peusumod string,
    cod_adm_secuencia bigint,
    dt_adm_pefecmod string,
    dec_adm_valoracion decimal(5, 2),
    fc_adm_patrim_neto decimal(16, 2),
    flag_adm_garantia_default string,
    cod_adm_moneda_garantia string,
    int_adm_opcion int,
    cod_adm_ltv bigint,
    dt_adm_vencimiento string,
    dec_adm_porcentaje_cobertura decimal(16, 4),
    cod_adm_codigo int,
    fc_adm_monto_limite decimal(16, 2)
)
PARTITIONED BY (
  partition_date string)
STORED AS PARQUET
LOCATION
  '${DATA_LAKE_SERVER}/santander/bi-corp/common/admision/stk_adm_empresas_garantias_lineas';