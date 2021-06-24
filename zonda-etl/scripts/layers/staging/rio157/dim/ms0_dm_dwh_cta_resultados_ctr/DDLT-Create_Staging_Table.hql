CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.rio157_ms0_dm_dwh_cta_resultados_ctr (
    cod_pais  string,
    cod_jerq_cr01  string,
    cod_cta_resultados  string,
    cod_cta_resultados_niv_1  string,
    cod_cta_resultados_niv_2  string,
    cod_cta_resultados_niv_3  string,
    cod_cta_resultados_niv_4  string,
    cod_cta_resultados_niv_5  string,
    cod_cta_resultados_niv_6  string,
    cod_cta_resultados_niv_7  string,
    cod_cta_resultados_niv_8  string,
    cod_cta_resultados_niv_9  string,
    cod_cta_resultados_niv_10  string,
    cod_cta_resultados_niv_11  string,
    cod_cta_resultados_niv_12  string,
    cod_cta_resultados_niv_13  string,
    cod_cta_resultados_niv_14  string,
    des_cta_resultados_niv_1  string,
    des_cta_resultados_niv_2  string,
    des_cta_resultados_niv_3  string,
    des_cta_resultados_niv_4  string,
    des_cta_resultados_niv_5  string,
    des_cta_resultados_niv_6  string,
    des_cta_resultados_niv_7  string,
    des_cta_resultados_niv_8  string,
    des_cta_resultados_niv_9  string,
    des_cta_resultados_niv_10  string,
    des_cta_resultados_niv_11  string,
    des_cta_resultados_niv_12  string,
    des_cta_resultados_niv_13  string,
    des_cta_resultados_niv_14  string,
    sig_cta_resultados_niv_1  string,
    sig_cta_resultados_niv_2  string,
    sig_cta_resultados_niv_3  string,
    sig_cta_resultados_niv_4  string,
    sig_cta_resultados_niv_5  string,
    sig_cta_resultados_niv_6  string,
    sig_cta_resultados_niv_7  string,
    sig_cta_resultados_niv_8  string,
    sig_cta_resultados_niv_9  string,
    sig_cta_resultados_niv_10  string,
    sig_cta_resultados_niv_11  string,
    sig_cta_resultados_niv_12  string,
    sig_cta_resultados_niv_13  string,
    sig_cta_resultados_niv_14  string,
    ind_balanceada_cta_resultados  string,
    userid_umo  string,
    timest_umo  string,
    cod_cta_resultados_niv_0  string,
    des_cta_resultados_niv_0  string,
    sig_cta_resultados_niv_0  string
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
      '${DATA_LAKE_SERVER}/santander/bi-corp/staging/rio157/dim/ms0_dm_dwh_cta_resultados_ctr'