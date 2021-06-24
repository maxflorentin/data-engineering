CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_common.stk_inv_fondos (
    cod_inv_sucursal_operativa int,
    cod_inv_cuenta int,
    cod_per_nup int,
    ds_inv_razon_social string,
    cod_inv_fondo int,
    ds_inv_fondo string,
    fc_inv_cuotapartes DECIMAL(17, 4),
    fc_inv_cuotapartes_bloq DECIMAL(17, 4),
    fc_inv_cotiz_fondo DECIMAL(17, 6),
    cod_inv_moneda string,
    fc_inv_impo_moneda_local DECIMAL(17, 2),
    fc_inv_impo_moneda_origen DECIMAL(17, 2),
    fc_inv_cotiz_moneda DECIMAL(15, 2),
    cod_inv_id_sociedad_gerente int,
    cod_inv_fondo_gerente string,
    cod_inv_fondo_clase string,
    cod_inv_isin string,
    cod_inv_cnv string,
    cod_inv_custodia string,
    cod_inv_formato_fondo string,
    int_inv_carencia int,
    cod_inv_tipo_fondo int
)
PARTITIONED BY (
  partition_date string)
STORED AS PARQUET
LOCATION
  '${DATA_LAKE_SERVER}/santander/bi-corp/common/inversiones/fact/stk_inv_fondos'