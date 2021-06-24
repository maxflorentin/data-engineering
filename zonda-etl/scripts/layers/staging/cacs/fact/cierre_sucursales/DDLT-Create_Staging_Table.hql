CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.cacs_mf_cierre_sucursales
(
        location_code string,
        acct_num string,
        sucurso string,
        location_coded string,
        acct_numd string,
        sucursd string,
        fecha_proceso string,
)
PARTITIONED BY(partition_date string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/staging/cacs/fact/cacs_mf_cierre_sucursales';