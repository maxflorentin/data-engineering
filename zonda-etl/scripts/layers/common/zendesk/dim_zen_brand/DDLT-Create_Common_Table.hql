CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_common.dim_zen_brand (
    cod_zen_brand BIGINT,
    ds_zen_url STRING,
    ds_zen_brand STRING,
    ds_zen_url_brand STRING,
    ds_zen_subdominio STRING,
    flag_zen_help_center INT,
    ds_estado_help_center STRING,
    flag_zen_estado INT,
    flag_zen_default INT,
    flag_zen_deleted INT,
    ds_zen_logo STRING,
    ds_zen_form STRING,
    ds_zen_signature_template STRING,
    ts_zen_creacion TIMESTAMP,
    ts_zen_actualizacion TIMESTAMP,
    ds_zen_host_mapping STRING,
    partition_date STRING
)
STORED AS PARQUET
LOCATION
  '${DATA_LAKE_SERVER}/santander/bi-corp/common/zendesk/dim/dim_zen_brand'
;