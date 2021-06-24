create external table if not exists bi_corp_bdr.sat_cliente_frbaja(
s1emp string,
idnumcli string,
fec_dato string,
clisegm string,
cod_sect string,
fec_baja_dato string,
mot_baja_dato string
)
PARTITIONED BY (partition_date string)
STORED AS PARQUET
LOCATION
  '/santander/bi-corp/bdr/dv/sat_cliente_frbaja';