CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_common.stk_mora_flujo_permanencia (
periodo string,
clave string,
nup bigint,
cod_sucursal bigint,
num_cuenta bigint,
nro_ingreso int,
salida_mora_anterior string,
ingreso_mora string,
salida_mora string,
meses_fuera_de_mora_hasta_ingresar int,
meses_en_mora_hasta_salir int,
ingreso_despues_de_12_meses_fuera_de_mora int,
inexistente_periodo_actual int,
ingreso_mora_lgd string,
meses_en_mora_lgd int,
ultimo_dia_del_periodo string,
pagos_consecutivos int,
marca_reingreso int,
actualizado  int,
contrato_migrado int
)
PARTITIONED BY (
  partition_date string)
STORED AS PARQUET
LOCATION
  '/santander/bi-corp/common/mora/stk_mora_flujo_permanencia'
;