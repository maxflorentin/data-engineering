CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.rio6_cart_causas_anulacion_recibos(
    cana_cd_causa_anulacion   string,
    cana_de_causa_anulacion   string,
    cana_in_rp                string,
    cana_tp_causa             string,
    cana_cd_causa_impr        string,
    cana_de_causa_impr        string,
    cana_cd_familia           string
)

PARTITIONED BY (partition_date string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/staging/rio6/dim/cart_causas_anulacion_recibos';