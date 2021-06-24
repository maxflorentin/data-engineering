create external table if not exists bi_corp_staging.alha9600(
entidad string,
fecha_informacion string,
rubro_altair string,
nombre_cuenta string,
rubro_bcra string,
cargabal string,                        
PdN string,
EM string,
cuenta_Ant string,
rubro_niif string,
enero string,
febrero string,
marzo string,
abril string,
mayo string,
junio string,
julio string,
agosto string,
septiembre string,
octubre string,
noviembre string,
diciembre string
)
PARTITIONED BY (partition_date string)
STORED AS PARQUET
LOCATION
  '${DATA_LAKE_SERVER}/santander/bi-corp/staging/manual/contab/alha9600';