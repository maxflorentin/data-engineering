CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_common.stk_adm_empresas_feve_acta_revision_clientes (
    id_adm_acta int,
    cod_per_nup int,
    id_adm_fichafeve bigint
)
PARTITIONED BY (
  partition_date string)
STORED AS PARQUET
LOCATION
  '${DATA_LAKE_SERVER}/santander/bi-corp/common/admision/stk_adm_empresas_feve_acta_revision_clientes';