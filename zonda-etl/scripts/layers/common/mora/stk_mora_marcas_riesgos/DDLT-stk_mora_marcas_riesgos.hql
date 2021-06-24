CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_common.stk_mora_marcas_riesgos (
periodo string,
nup string,
entidad string,
centro string ,
contrato string,
producto string,
subproducto string,
divisa string,
marca_cura_pcr09 string,
marca_cura_pcr16 string,
mora_pcr09 string,
mora_pcr16 string,
default_pd_pcr09 string,
default_pd_pcr16 string,
fecha_entrada_mora404 string,
fecha_entrada_mora404_pcr16 string,
distribucion_contratos string,
meses_en_mora int,
meses_en_mora_lgd int,
meses_en_mora_pcr16 int,
meses_en_mora_lgd_pcr16 int,
rating_fecha_alta_contrato decimal(5,2),
vigilancia_especial string,
subestandar string,
fecha_entrada_subestandar string,
distribucion_generica string,
fecha_alta_contrato string,
segmento string,
imp_riesgo decimal(17,4),
imp_deuda decimal(17,4),
cuadrante string,
tipo_persona string,
segmento_control string,
descripcion_producto string,
deuda_total_en_mora decimal(17,4),
deuda_total_en_mora_pcr16 decimal(17,4),
deuda_total_cliente decimal(17,4),
porcent_para_default_pd_pcr09 decimal(7,4),
porcent_para_default_pd_pcr16 decimal(7,4),
licuado_de_mora_lg string,
contrato_covid string
)
PARTITIONED BY (
  partition_date string)
STORED AS PARQUET
LOCATION
  '/santander/bi-corp/common/mora/stk_mora_marcas_riesgos'
;