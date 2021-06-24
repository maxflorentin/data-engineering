create external table if not exists bi_corp_staging.tactico_productos_dist_generica(
producto string,
subproducto string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\;'
STORED AS TEXTFILE
LOCATION '/santander/bi-corp/common/mora/tactico_productos_dist_generica';