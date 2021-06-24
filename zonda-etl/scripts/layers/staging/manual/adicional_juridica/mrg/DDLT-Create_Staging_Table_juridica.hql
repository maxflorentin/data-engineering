create external table if not exists bi_corp_staging.no_mrg_juridica(
fecha string,
empresa  string,
exposicion  string,
calificada_localmente  string,
identificador_cliente  string,
fecha_informacion  string,
facturacion  string,
total_activos_cliente  string,
numero_empleados  string,
origen_facturacion string,
origen_activos string,
cod_cargabal string,
total_deuda_cliente string,
marca_facturacion string,
rating string
)
PARTITIONED BY (partition_date string)
STORED AS PARQUET
LOCATION
  '${DATA_LAKE_SERVER}/santander/bi-corp/staging/manual/adicional_juridica/mrg_juridica';