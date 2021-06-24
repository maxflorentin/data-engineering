CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.rio265_challenge
    (
        id STRING ,
        name STRING ,
        order STRING ,
        description STRING ,
        user STRING ,
        pointsduration STRING ,
        datefrom STRING ,
        dateto STRING ,
        hasautomaticrenewal STRING ,
        originaldatefrom STRING ,
        originaldateto STRING ,
        triggerpoints STRING ,
        id_trigger STRING ,
        id_bonuscode STRING ,
        id_challengesfather STRING ,
        id_challengestate STRING ,
        only_one_time STRING
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
      '${DATA_LAKE_SERVER}/santander/bi-corp/staging/rio265/dim/rio265_challenge'