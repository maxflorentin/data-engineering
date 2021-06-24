create external table if not exists bi_corp_staging.no_mrg_grupos(
fecha string,
empresa  string,
calificada_localmente  string,
identificador_grupo_economico  string,
nombre  string,
pais  string,
facturacion_grupo  string,
total_activos  string,
numero_de_empleados  string,
fecha_informacion string,
sector string,
sector_grupo_economico string
)
PARTITIONED BY (partition_date string)
STORED AS PARQUET
LOCATION
  '${DATA_LAKE_SERVER}/santander/bi-corp/staging/manual/adicional_juridica/mrg_grupos';