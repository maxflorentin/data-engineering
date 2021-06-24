CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.rio258_cp_productos_crupier(
        cod_producto string ,
        desc_producto string
        -- ultima_modif string
 )
    PARTITIONED BY (
    ultima_modif string )
    ROW FORMAT SERDE
      'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
    STORED AS INPUTFORMAT
      'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
    OUTPUTFORMAT
      'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
    LOCATION
      '${DATA_LAKE_SERVER}/santander/bi-corp/staging/rio258/dim/rio258_cp_productos_crupier'