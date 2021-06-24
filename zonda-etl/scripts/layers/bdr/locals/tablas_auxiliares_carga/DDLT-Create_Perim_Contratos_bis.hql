create external table if not exists bi_corp_bdr.perim_contratos_bis(
id_cto_bdr string,
id_cto_source string
)
partitioned by (partition_date string)
stored as parquet
location '${DATA_LAKE_SERVER}/santander/bi-corp/bdr/perim_contratos_bis';