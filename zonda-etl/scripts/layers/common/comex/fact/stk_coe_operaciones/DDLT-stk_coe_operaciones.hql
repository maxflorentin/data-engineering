
CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_common.stk_coe_operaciones
(
cod_coe_operacion string,
cod_coe_nrooperacion string,
ds_coe_secuencia string,
dt_coe_fecha_operacion string,
ds_coe_estado_operacion string,
cod_coe_producto string,
cod_coe_moneda string,
ds_coe_canal string,
cod_per_nup string,
cod_coe_oficial_opera string,
cod_coe_oficial_rechaza_inc string,
cod_coe_oficial_relanza string,
fc_coe_importe decimal(15,2),
cod_coe_bco_corresponsal string,
ds_coe_concepto string,
cod_coe_concepto string,
cod_coe_sucursal string,
ds_coe_sucursal string,
ds_coe_segmento string,
ds_coe_subsegmento string,
cod_coe_cuadrante string,
flag_coe_uge int,
ds_coe_oficial_opera string,
ds_coe_oficial_rechaza_inc string,
ds_coe_ofcial_relanza string,
ds_coe_moneda string,
ds_coe_producto string,
cod_coe_motivo_rechazo string,
ds_coe_motivo_rechazo_interno string,
ds_coe_motivo_rechazo_externo string,
cod_coe_agrupador_motivo string,
ds_coe_pais_origen string,
cod_coe_pais_origen string,
ds_coe_pais_beneficiario string,
cod_coe_pais_beneficiario string,
cod_coe_celula string,
ds_coe_celula string,
cod_coe_tipo_celula string,
ds_coe_tipo_celula string
)

PARTITIONED BY (
  partition_date string)
STORED AS PARQUET
LOCATION
  '${DATA_LAKE_SERVER}/santander/bi-corp/common/comex/fact/coe_operaciones'
