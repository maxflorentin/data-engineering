CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_common.stk_adm_empresas_resumen_crediticio (
    cod_adm_numero_propuesta int,
    cod_adm_numero_persona_sge int,
    int_adm_anio_ganancias int,
    fc_adm_total_ingresos_gravados decimal(30, 2),
    fc_adm_resultado_neto decimal(30, 2),
    fc_adm_patrimonio_neto decimal(30, 2),
    dt_adm_fec_balance_ddjj string,
    dt_adm_pefecalt string,
    dt_adm_pefecmod string,
    int_adm_orden int,
    ds_adm_periodo string
)
PARTITIONED BY (
  partition_date string)
STORED AS PARQUET
LOCATION
  '${DATA_LAKE_SERVER}/santander/bi-corp/common/admision/stk_adm_empresas_resumen_crediticio';