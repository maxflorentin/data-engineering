create external table if not exists bi_corp_bdr.tablas_info(
tabla string,
longitud string,
header string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\;'
STORED AS TEXTFILE
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/bdr/tablas_info';