CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.rio157_ms0_dm_je_dwh_segmentos_ctr (
    cod_pais  string,
    cod_padre  string,
    cod_hijo  string,
    des_hijo  string,
    cod_jerarquia  string,
    ind_consolidacion  string,
    cod_primer_corporativo  string,
    userid_umo  string,
    timest_umo  string,
    num_nivel  string,
    ind_corporativo  string,
    num_orden  string
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
      '${DATA_LAKE_SERVER}/santander/bi-corp/staging/rio157/dim/ms0_dm_je_dwh_segmentos_ctr'