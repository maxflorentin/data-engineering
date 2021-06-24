create external table if not exists bi_corp_bdr.perim_mdr_contraparte(
nup  string,
shortname string
)
partitioned by (partition_date string)
stored as parquet
location '${DATA_LAKE_SERVER}/santander/bi-corp/bdr/perim_mdr_contraparte';
