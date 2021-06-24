
CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_common.dim_mora_primas_genericas (
periodo string,
porc_cuenta_cte string,
porc_personales string,
porc_tarjetas string,
porc_otros string,
porc_hipotecario string,
porc_prendario string,
distribucion string
)
PARTITIONED BY (
  partition_date string)
STORED AS PARQUET
LOCATION
  '/santander/bi-corp/common/mora/dim_mora_primas_genericas'
;