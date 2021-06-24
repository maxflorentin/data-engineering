CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.rio6_cart_fr_pagos_productos(
    capg_carp_cd_ramo       string,
    capg_capu_cd_producto   string,
    capg_cafp_cd_fr_pago    string
)

PARTITIONED BY (partition_date string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/staging/rio6/dim/cart_fr_pagos_productos';