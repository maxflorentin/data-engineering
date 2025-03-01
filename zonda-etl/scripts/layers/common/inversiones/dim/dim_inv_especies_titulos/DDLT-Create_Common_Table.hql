CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_common.dim_inv_especies_titulos (
cod_inv_especie string,
cod_inv_oid_especie bigint,
ds_inv_tipo_especie string,
ds_inv_especie string,
cod_inv_rossi string,
cod_inv_bce string,
cod_inv_isin string,
cod_inv_sam string,
cod_inv_bcra string,
cid_inv_cvsa string,
cod_inv_atit string,
cod_inv_mae string,
cod_inv_fm string,
cod_inv_amigo string,
cod_inv_pais_emisor string,
cod_inv_moneda_emision string,
dt_inv_fecha_cup_proxima string,
dt_inv_fecha_cup_anterior string,
dt_inv_fecha_vencimiento string,
cod_inv_periodicidad_pago string,
cod_inv_tipo_tasa string,
fc_inv_tasa bigint,
fc_inv_vr bigint,
fc_inv_lam_min bigint,
fc_inv_min_incre bigint,
cod_inv_cash_flow string,
cod_inv_tipo_cap_precio string,
cod_inv_precio_ej string,
dt_inv_fecha_ej string,
cod_inv_per_de_liq string,
cod_inv_cant_por_lote bigint,
dt_inv_dividend_date string,
cod_inv_dividend string,
cod_inv_amb_negociacion string,
ds_inv_observaciones string,
cod_inv_estado string,
cod_inv_subespecie string,
dt_inv_fecha_precio string,
cod_inv_plazo string,
cod_inv_titulo_publico_o_privado bigint,
cod_inv_lugar_default_custodia string,
cod_inv_sic string,
cod_inv_fte_de_precio string,
cod_inv_tipo_valorizacion string,
cod_inv_moneda_cotizacion string,
cod_inv_clasificacion_extracto string,
cod_inv_basis string,
cod_inv_unsolicited string,
cod_inv_prodtype string,
cod_inv_intereses_corridos string,
cod_inv_producto bigint,
cod_inv_categoria_cv bigint,
cod_inv_obra_publica string,
dt_inv_fecha_hasta_amb_neg string,
cod_inv_ajuste_capital string,
cod_inv_indice bigint
)
PARTITIONED BY (
  partition_date string)
STORED AS PARQUET
LOCATION
  '${DATA_LAKE_SERVER}/santander/bi-corp/common/inversiones/dim/dim_inv_especies_titulos'