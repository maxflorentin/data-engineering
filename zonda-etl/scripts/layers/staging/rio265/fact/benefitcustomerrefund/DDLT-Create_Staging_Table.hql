CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.rio265_benefitcustomerrefund
    (
        id STRING ,
        id_benefitcustomer STRING ,
        id_transactiontype STRING ,
        id_coin STRING ,
        totalamount STRING ,
        transactiondate STRING,
        cardnumber STRING,
        receiptnumber STRING,
        id_establishment STRING,
        discountamount STRING
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
      '${DATA_LAKE_SERVER}/santander/bi-corp/staging/rio265/fact/rio265_benefitcustomerrefund'