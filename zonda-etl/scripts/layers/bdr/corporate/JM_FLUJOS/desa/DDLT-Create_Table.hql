create external table if not exists bi_corp_bdr.test_jm_flujos(
r9746_feoperac string,
r9746_s1emp  string,
r9746_contra1 string,
r9746_fecmvto string,
r9746_clasmvto string,
r9746_importe string,
r9746_salonbal string,
origen string
)
partitioned by (partition_date string)
stored as parquet
location '${DATA_LAKE_SERVER}/santander/bi-corp/bdr/desa/test_jm_flujos';