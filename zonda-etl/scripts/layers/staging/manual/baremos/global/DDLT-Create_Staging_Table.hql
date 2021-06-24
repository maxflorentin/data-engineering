create external table if not exists bi_corp_bdr.map_baremos_global_local (
empresa string,
cod_negocio string,
cod_baremo_global string,
cod_baremo_local string,
fecha_desde string,
fecha_hasta string,
fecha_grabacion string,
fecha_modificacion string,
cod_negocio_local string
)
STORED AS PARQUET
LOCATION
  '${DATA_LAKE_SERVER}/santander/bi-corp/bdr/map_baremos_global_local';