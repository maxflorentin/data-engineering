CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.rio6_cart_fr_pagos(
    cafp_cd_fr_pago          string,
    cafp_de_fr_pago          string,
    cafp_nu_pagos_ano        string,
    cafp_tp_emision          string,
    cafp_nu_meses_fr_cobro   string,
    cafp_nu_meses_vig        string,
    cafp_nu_cuotas_cobrar    string
)

PARTITIONED BY (partition_date string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/staging/rio6/dim/cart_fr_pagos';