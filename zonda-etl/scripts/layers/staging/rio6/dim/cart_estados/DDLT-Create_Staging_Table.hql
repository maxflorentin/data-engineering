CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.rio6_cart_estados(
    caes_cd_estado             string,
    caes_de_estado             string,
    caes_po_ingresos_brutos    string,
    caes_mt_minimo_imponible   string
)

PARTITIONED BY (partition_date string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/staging/rio6/dim/cart_estados';