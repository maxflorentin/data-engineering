create external table if not exists bi_corp_bdr.test_balances_ajustes(
cargabal string,
sociedad string,
sociedad_eliminacion string,
importe string
)
partitioned by (partition_date string)
stored as parquet
location '${DATA_LAKE_SERVER}/santander/bi-corp/bdr/test_balances_ajustes';