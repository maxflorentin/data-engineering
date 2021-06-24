create external table if not exists bi_corp_bdr.long_controles(
periodo string,
longitud string,
detalle string,
op_timestamp string
)
PARTITIONED BY (`tabla` string)
STORED AS PARQUET
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/bdr/long_controles';