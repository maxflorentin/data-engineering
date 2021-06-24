CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.rio6_cart_coberturas(
cacb_carb_cd_ramo                 string,
cacb_cd_cobertura                 string,
cacb_de_cobertura                 string,
cacb_carp_cd_ramo                 string,
cacb_caro_cd_ramo                 string,
cacb_mt_maximo                    string,
cacb_in_sumarizacion              string,
cacb_in_cobertura_sub             string,
cacb_de_cobertura1                string,
cacb_de_cobertura2                string,
cacb_de_cobertura3                string,
cacb_in_aumento_automatico        string,
cacb_mt_maximo_dolares            string,
cacb_in_valida_monto_sin          string,
cacb_in_sumapri                   string,
cacb_in_restitucion               string,
cacb_cd_producto                  string
)

PARTITIONED BY (partition_date string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/staging/rio6/fact/cart_coberturas';