CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_bdr.normalizacion_id_clientes(
id_cte_bdr string,
id_cte_source string,
source string,
inserted_date string
)
STORED AS PARQUET
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/bdr/normalizacion_id_clientes';