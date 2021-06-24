CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_common.stk_inv_titulos (
cod_inv_sucursal_operativa  int,
cod_inv_cuenta              bigint,
cod_per_nup                 bigint,
cod_inv_especie             string,
ds_inv_tipo_especie         string,
ds_inv_especie              string,
cod_inv_moneda_emision      string,
cod_inv_cantidad            decimal(25,7),
cod_inv_precio              decimal(25,7),
cod_inv_importe             decimal(25,7),
cod_inv_periodicidad_pago   string,
cod_inv_producto            bigint,
cod_inv_estado_tenencia     string,
cod_inv_lugar_titulos       string
)
PARTITIONED BY (
  partition_date string)
STORED AS PARQUET
LOCATION
  '${DATA_LAKE_SERVER}/santander/bi-corp/common/inversiones/fact/stk_inv_titulos'