create external table if not exists bi_corp_bdr.perim_rating_aqua(
fec_dato string,
fec_periodo string,
num_persona string,
fec_autoriz_rating_act string,
coef_rating string
)
partitioned by (partition_date string)
stored as parquet
location '${DATA_LAKE_SERVER}/santander/bi-corp/bdr/perim_rating_aqua';
