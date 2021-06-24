create external table if not exists bi_corp_bdr.perim_arda_contratos(
num_persona string,
id_cto_bdr string,
id_cto_contratos_sin_divisa string
)
partitioned by (partition_date string)
stored as parquet
location '${DATA_LAKE_SERVER}/santander/bi-corp/bdr/perim_arda_contratos';