CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.academia_view_users_data (
     id  string,
        nombre string,
        legajo string,
        puesto string,
        centro_costos string,
        centro_costos_nombre string
)
PARTITIONED BY(partition_date string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/staging/academia/academia_view_users_data';