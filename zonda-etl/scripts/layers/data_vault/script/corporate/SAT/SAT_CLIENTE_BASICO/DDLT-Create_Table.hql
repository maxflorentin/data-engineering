create external table if not exists bi_corp_bdr.sat_cliente_basico(
s1emp string,
idnumcli string,
fec_dato string,
tip_pers string,
apnomper string,
datexto1 string,
datexto2 string,
identper string,
codidper string,
clie_glo string,
fecnacim string,
fec_baja_dato string,
mot_baja_dato string
)
PARTITIONED BY (partition_date string)
STORED AS PARQUET
LOCATION
  '/santander/bi-corp/bdr/dv/sat_cliente_basico';