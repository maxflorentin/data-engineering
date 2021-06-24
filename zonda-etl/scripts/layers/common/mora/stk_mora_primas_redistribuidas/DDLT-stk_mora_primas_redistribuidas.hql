CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_common.stk_mora_primas_redistribuidas (
nup bigint,
cod_sucursal bigint,
num_cuenta bigint,
cod_producto string,
cod_subproducto string,
cod_divisa string,
descripcion string,
categoria_particulares string,
categoria_carterizados string,
refinanciacion string,
tipo_reestructuracion string,
imp_total string,
redistribuido_cuenta_corriente string,
redistribuido_tarjetas_credito string,
redistribuido_prestamos_personales string,
redistribuido_otros string,
redistribuido_hipotecarios string,
redistribuido_prendarios string,
distribuido_por_cierre_controlado string,
distribuido_por_contratos_ref string,
distribucion_generica string
)
PARTITIONED BY (
partition_date string)
STORED AS PARQUET
LOCATION
'/santander/bi-corp/common/mora/stk_mora_primas_redistribuidas'
;