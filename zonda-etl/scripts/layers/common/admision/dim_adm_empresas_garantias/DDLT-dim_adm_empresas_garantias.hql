CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_common.dim_adm_empresas_garantias (
    ds_adm_pecdgent string,
    ds_adm_peidioma string,
    cod_adm_gartia string,
    ds_adm_des_gartia string,
    dec_adm_porc_max decimal(16,2),
    ds_adm_peusualt string,
    ds_adm_peusumod string,
    dt_adm_pefecalt string,
    dt_adm_pefecmod string,
    int_adm_puntaje int,
    ds_adm_categoria string,
    ds_adm_tipo string,
    int_adm_grupo_gartia int,
    ds_adm_des_garantia_bma string,
    ds_adm_gtia_clean string
)
PARTITIONED BY (
  partition_date string)
STORED AS PARQUET
LOCATION
  '${DATA_LAKE_SERVER}/santander/bi-corp/common/admision/dim_adm_empresas_garantias';
