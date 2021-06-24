CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_common.stk_adm_empresas_ddjj_propuestas (
    id_adm_ddjj int,
    cod_adm_numero_propuesta int,
    dt_adm_fecha string,
    ds_adm_periodo_ddjj string,
    fc_adm_resultado_neto decimal(15,2),
    fc_adm_amortizaciones decimal(15,2),
    fc_adm_ventas_anuales decimal(15,2),
    fc_adm_patrimonio_neto_total decimal(15,2),
    fc_adm_deuda_bancaria decimal(15,2),
    int_adm_orden int
)
PARTITIONED BY (
  partition_date string)
STORED AS PARQUET
LOCATION
  '${DATA_LAKE_SERVER}/santander/bi-corp/common/admision/stk_adm_empresas_ddjj_propuestas';