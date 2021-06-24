CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_common.dim_adm_empresas_actividades_santander_nace (
    cod_adm_act int,
    cod_adm_nace string
)
PARTITIONED BY (
  partition_date string)
STORED AS PARQUET
LOCATION
  '${DATA_LAKE_SERVER}/santander/bi-corp/common/admision/dim_adm_empresas_actividades_santander_nace';