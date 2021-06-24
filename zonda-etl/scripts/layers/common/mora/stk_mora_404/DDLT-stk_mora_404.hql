
CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_common.stk_mora_404 (
periodo string,
cod_entidad string,
nup bigint,
cod_sucursal bigint,
num_cuenta bigint,
cod_producto string,
cod_subproducto string,
cod_divisa string,
fecha_situacion_irregular string,
fecha_alta_producto string,
fecha_vto_producto string,
dias_de_atraso_calculado string,
dias_de_atraso int,
cod_marca string,
cod_submarca string,
cod_segmento string,
segmento_control string,
detalle_renta string,
tipo_producto string,
descripcion_producto string,
tipo_reestructuracion string,
motivo_riesgo_subestado string,
clasificacion_reestructuracion string,
fecha_clasificacion_reestructuracion string,
via_ingreso string,
importe_riesgo DECIMAL(17,4),
importe_irregular DECIMAL(17,4)
)
PARTITIONED BY (
  partition_date string)
STORED AS PARQUET
LOCATION
  '/santander/bi-corp/common/mora/stk_mora_404'
;