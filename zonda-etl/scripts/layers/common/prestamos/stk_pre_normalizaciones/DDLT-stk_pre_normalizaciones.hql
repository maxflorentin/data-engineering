CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_common.stk_pre_normalizaciones (
cod_pre_entidad string,
cod_suc_sucursal int,
cod_nro_cuenta bigint,
cod_per_nup int,
cod_pre_nrosecuencia int,
cod_pre_nroree int,
cod_prod_producto string,
cod_prod_subproducto string,
cod_div_divisa string,
cod_pre_marcaclasificacionactual string,
dt_pre_fechaapertura string,
dt_pre_fechacambioclasificacion string,
dt_pre_fechacura string,
dt_pre_fechanormalizacion string
)
PARTITIONED BY (
  partition_date string)
STORED AS PARQUET
LOCATION
  '${DATA_LAKE_SERVER}/santander/bi-corp/common/prestamos/stk_pre_normalizaciones';