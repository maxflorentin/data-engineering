create external table if not exists bi_corp_staging.corresponsales(
nup string,
banco string,
pais string,
cod_afip string,
sector_contable string,
tipo_segmento_local_1 string,                        
tipo_segmento_local_2  string,
segmento_cliente string,
moneda string,
cargabal string,
rubro_altair string,
rubro_altair_ant string,
rubro_bcra string,
agrupador_producto string
)
PARTITIONED BY (partition_date string)
STORED AS PARQUET
LOCATION
  '${DATA_LAKE_SERVER}/santander/bi-corp/staging/manual/contab/corresponsales';