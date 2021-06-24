CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_common.stk_adm_empresas_feve_acta_revision_asistentes (
    id_adm_acta int,
    ds_adm_nombrecompleto string,
    cod_adm_dni bigint,
    cod_adm_cargo string,
    cod_per_nup int
)
PARTITIONED BY (
  partition_date string)
STORED AS PARQUET
LOCATION
  '${DATA_LAKE_SERVER}/santander/bi-corp/common/admision/stk_adm_empresas_feve_acta_revision_asistentes';