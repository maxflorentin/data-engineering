CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_common.stk_adm_empresas_garantias_operacion (
    cod_adm_penumper bigint,
    int_adm_secu_f487 int,
    int_adm_secu_garan_ope int,
    cod_adm_gartia string,
    ds_adm_tipo string,
    int_adm_porc_garantia int,
    fc_adm_monto_garantia decimal(16,2),
    ds_adm_detalle_garantia string,
    ds_adm_peusualt string,
    ts_adm_pefecalt string,
    ts_adm_pefecmod string,
    ds_adm_peusumod string,
    id_adm_garantia_altair int,
    int_adm_porc_gtia int,
    int_adm_secuencia int,
    dt_adm_fecha_vigencia string,
    ds_adm_moneda_gartia string,
    fc_adm_monto_limite decimal(16,2)
)
PARTITIONED BY (
  partition_date string)
STORED AS PARQUET
LOCATION
  '/santander/bi-corp/common/admision/stk_adm_empresas_garantias_operacion';