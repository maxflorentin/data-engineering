CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_common.dim_zen_form (
        cod_zen_form BIGINT,
        ds_zen_url STRING,
        ds_zen_form STRING,
        ds_zen_display STRING,
        flag_zen_usuario_visible INT,
        cod_zen_posicion INT,
        ds_zen_grupo_fields STRING,
        flag_zen_estado INT,
        flag_zen_default INT,
        ts_zen_creacion TIMESTAMP,
        ts_zen_actualizacion TIMESTAMP,
        flag_zen_todos_los_brands INT,
        ds_zen_brand_restricted STRING,
        ds_zen_condiciones_usuario_final STRING,
        partition_date STRING
)
STORED AS PARQUET
LOCATION
  '${DATA_LAKE_SERVER}/santander/bi-corp/common/zendesk/dim/dim_zen_forms'
;