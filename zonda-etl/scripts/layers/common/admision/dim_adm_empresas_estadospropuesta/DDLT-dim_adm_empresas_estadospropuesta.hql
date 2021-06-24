CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_common.dim_adm_empresas_estadospropuesta (
    cod_adm_estado string,
    ds_adm_estado string
)
PARTITIONED BY (
  partition_date string)
STORED AS PARQUET
LOCATION
  '${DATA_LAKE_SERVER}/santander/bi-corp/common/admision/dim_adm_empresas_estadospropuesta';