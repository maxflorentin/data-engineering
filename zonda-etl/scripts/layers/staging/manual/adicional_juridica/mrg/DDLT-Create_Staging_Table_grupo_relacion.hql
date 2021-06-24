create external table if not exists bi_corp_staging.no_mrg_grupo_relacion(
fecha string,
empresa  string,
nombre_grupo  string,
identificador_grupo_economico  string,
identificador_cliente  string,
rol_jerarquico  string
)
PARTITIONED BY (partition_date string)
STORED AS PARQUET
LOCATION
  '${DATA_LAKE_SERVER}/santander/bi-corp/staging/manual/adicional_juridica/mrg_grupo_relacion';