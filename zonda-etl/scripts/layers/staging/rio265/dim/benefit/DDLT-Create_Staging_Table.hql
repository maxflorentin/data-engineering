CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.rio265_benefit
    (
        id STRING ,
        name STRING ,
        description STRING ,
        user STRING ,
        datefrom STRING ,
        dateto STRING ,
        authorizationuser STRING ,
        modificationuser STRING ,
        hasautomaticrenewal STRING ,
        hasretroactiveedition STRING ,
        termsandconditions STRING ,
        originaldatefrom STRING ,
        originaldateto STRING ,
        investment STRING ,
        id_award STRING ,
        id_benefitstate STRING ,
        pausedate STRING
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
      '${DATA_LAKE_SERVER}/santander/bi-corp/staging/rio265/dim/rio265_benefit'