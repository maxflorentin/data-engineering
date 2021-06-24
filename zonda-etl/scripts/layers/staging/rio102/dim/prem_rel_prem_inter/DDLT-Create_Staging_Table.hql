CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.rio102_prem_rel_prem_inter(
        id_premiacion string,
        id_interfaz string,
        id_col_sum string
)
PARTITIONED BY (partition_date string)
ROW FORMAT SERDE
    'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
 STORED AS INPUTFORMAT
    'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
 OUTPUTFORMAT
    'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
 LOCATION
    '${DATA_LAKE_SERVER}/santander/bi-corp/staging/rio102/dim/rio102_prem_rel_prem_inter';