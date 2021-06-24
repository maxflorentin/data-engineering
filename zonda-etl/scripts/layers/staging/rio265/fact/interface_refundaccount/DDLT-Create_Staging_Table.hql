CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.rio265_interface_refundaccount
    (
        id STRING,
        id_benefitcustomer STRING,
        nup STRING,
        dateused STRING,
        transactiondate STRING,
        entity STRING,
        office STRING,
        account STRING,
        coin STRING,
        operationalcode STRING,
        observations STRING,
        amount STRING
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
      '${DATA_LAKE_SERVER}/santander/bi-corp/staging/rio265/fact/rio265_interface_refundaccount'