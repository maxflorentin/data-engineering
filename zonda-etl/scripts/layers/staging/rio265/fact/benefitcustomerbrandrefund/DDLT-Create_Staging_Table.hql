CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.rio265_benefitcustomerbrandrefund
    (
        as_of_date STRING,
        v_idbenefitcustomerrefund STRING,
        id STRING,
        id_benefitbrandrefund STRING,
        id_benefitrefund STRING,
        nup STRING,
        id_level STRING,
        operationalcode STRING,
        amount STRING,
        discount STRING,
        id_transactiontype STRING,
        receiptnumber STRING,
        id_atm STRING,
        id_establishment STRING,
        category STRING,
        entity STRING,
        office STRING,
        coin STRING,
        observations STRING,
        paymentmethod STRING,
        paymentmethodcard STRING,
        account STRING,
        cardnumber STRING,
        campaigncode STRING,
        accreditationpercentage STRING,
        commercepercentage STRING,
        bankpercentage STRING,
        topamount STRING,
        transactiondate STRING,
        monday STRING,
        tuesday STRING,
        wednesday STRING,
        thursday STRING,
        friday STRING,
        saturday STRING,
        sunday STRING,
        expiry STRING,
        toptransactions STRING
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
      '${DATA_LAKE_SERVER}/santander/bi-corp/staging/rio265/fact/rio265_benefitcustomerbrandrefund'