create external table if not exists bi_corp_bdr.marca_utp_clientes
(
num_persona string,
max_fec_inicio_vigencia string
)
partitioned by (`partition_date` string)
stored as parquet
location '${DATA_LAKE_SERVER}/santander/bi-corp/bdr/marca_utp_clientes';