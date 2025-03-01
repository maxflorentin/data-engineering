CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.rio265_challengecustomer
    (
        id STRING ,
        id_challenge STRING ,
        nup STRING ,
        expireddate STRING ,
        points STRING ,
        dateused STRING ,
        id_level STRING ,
        id_request STRING
        )
    PARTITIONED BY (
      partition_date string)
    ROW FORMAT SERDE
      'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
    STORED AS INPUTFORMAT
      'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
    OUTPUTFORMAT
      'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
    LOCATION
      '${DATA_LAKE_SERVER}/santander/bi-corp/staging/rio265/fact/rio265_challengecustomer'