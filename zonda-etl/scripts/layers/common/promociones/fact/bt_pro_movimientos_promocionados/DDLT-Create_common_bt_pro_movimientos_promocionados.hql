CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_common.bt_pro_movimientos_promocionados
(
cod_pro_nupconsumo int,
cod_pro_nuptitular int,
cod_pro_cuenta string,
cod_pro_tipo_cuenta string,
cod_pro_tarjeta string,
cod_pro_sucursal string,
dt_pro_movimiento string,
ts_pro_movimiento string,
cod_pro_establecimiento string,
ds_pro_establecimiento string,
cod_pro_comercio string,
ds_pro_comercio string,
cod_pro_camp string,
ds_pro_campania string,
cod_pro_rubro string,
fc_pro_importebonificado decimal(15,2),
fc_pro_importeconsumo decimal(15,2),
ds_pro_mediopago string)
PARTITIONED BY (
  partition_date string)
STORED AS PARQUET
LOCATION
  '${DATA_LAKE_SERVER}/santander/bi-corp/common/promociones/fact/bt_pro_movimientos_promocionados'
