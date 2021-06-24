create external table if not exists bi_corp_bdr.hub_cliente(
s1emp string,
idnumcli string,
fec_dato string
)
PARTITIONED BY (partition_date string)
STORED AS PARQUET
LOCATION
  '/santander/bi-corp/bdr/dv/hub_cliente';