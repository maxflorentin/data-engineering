create external table if not exists bi_corp_bdr.perim_contratos_bis_covid(
g4095_contra1 string,
g4095_fechaper string,
g4095_feccance string,
g4095_coddiv string,
g4095_idfinald string,
contr_partition_date string
)
partitioned by (partition_date string)
stored as parquet
location '${DATA_LAKE_SERVER}/santander/bi-corp/bdr/perim_contratos_bis_covid';