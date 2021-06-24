create external table if not exists bi_corp_bdr.baremos_local (
empresa string,
cod_negocio_local string,
cod_baremo_local string,
fecha_desde string,
fecha_hasta string,
desc_baremo_local string,
fecha_modificacion string,
cod_baremo_alfanumerico_local string
)
STORED AS PARQUET
LOCATION
  '${DATA_LAKE_SERVER}/santander/bi-corp/bdr/baremos_local';