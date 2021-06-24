CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_common.dim_zen_field (
    cod_zen_field BIGINT,
    ds_zen_url STRING,
    ds_zen_titulo STRING,
    ds_zen_descripcion_detalle STRING,
    cod_zen_posicion INT,
    flag_zen_estado INT,
    flag_zen_requerido INT,
    flag_zen_collapsed_for_agents INT,
    cod_zen_regexp_for_validation STRING,
    ds_zen_titulo_portal STRING,
    flag_zen_visible_en_portal INT,
    flag_zen_editable_en_portal INT,
    flag_zen_requerido_en_portal INT,
    ds_zen_etiqueta STRING,
    ts_zen_creacion TIMESTAMP,
    ts_zen_actualizacion TIMESTAMP,
    flag_zen_removable INT,
    ds_zen_agente STRING,
    partition_date STRING
)
STORED AS PARQUET
LOCATION
  '${DATA_LAKE_SERVER}/santander/bi-corp/common/zendesk/dim/dim_zen_field'
;