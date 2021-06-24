CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_common.dim_adm_empresas_relacion_lineas_productos (
    cod_prod_producto string,
    cod_prod_subproducto string,
    cod_adm_operacion string,
    cod_adm_peusualt string,
    ts_adm_pefecalt string,
    cod_adm_peusumod string,
    ts_adm_pefecmod string
)
PARTITIONED BY (
  partition_date string)
STORED AS PARQUET
LOCATION
  '${DATA_LAKE_SERVER}/santander/bi-corp/common/admision/dim_adm_empresas_relacion_lineas_productos';