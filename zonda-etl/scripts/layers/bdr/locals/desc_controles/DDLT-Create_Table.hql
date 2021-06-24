create external table if not exists bi_corp_bdr.desc_controles(
descripcion string,
cod_incidencia string,
tabla string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\;'
STORED AS TEXTFILE
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/bdr/desc_controles';