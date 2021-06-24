CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.rio265_customer
    (
        nup STRING ,
        last_name STRING ,
        first_name STRING ,
        id_level STRING ,
        points STRING ,
        docnumber STRING ,
        doctype STRING ,
        birthdate STRING ,
        id_audience STRING ,
        accepted_tyc STRING
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
      '${DATA_LAKE_SERVER}/santander/bi-corp/staging/rio265/dim/rio265_customer'