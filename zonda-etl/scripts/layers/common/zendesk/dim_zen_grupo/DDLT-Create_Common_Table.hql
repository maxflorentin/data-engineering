CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_common.dim_zen_grupo (
    cod_zen_grupo BIGINT,
    ds_zen_url STRING,
    ds_zen_grupo STRING,
    ds_zen_descripcion STRING,
    flag_zen_default INT,
    flag_zen_deleted INT,
    ts_zen_creacion TIMESTAMP,
    ts_zen_actualizacion TIMESTAMP,
    partition_date STRING
)
STORED AS PARQUET
LOCATION
  '${DATA_LAKE_SERVER}/santander/bi-corp/common/zendesk/dim/dim_zen_grupo'
;
