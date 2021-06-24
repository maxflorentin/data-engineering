CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.aqua_rating_emisiones(
        isin string ,
        rtg_moody string ,
        rtg_moody_wt string ,
        rtg_moody_dt string ,
        rtg_moody_shrt string ,
        rtg_moody_shrt_wt string ,
        rtg_mdy_shrt_rating_dt string ,
        rtg_moody_long string ,
        rtg_mdy_long_wt string ,
        rtg_mdy_long_rating_dt string ,
        rtg_mdy_issuer_rating string ,
        rtg_mdy_issuer_rating_wt string ,
        rtg_mdy_issuer_rtg_dt string ,
        rtg_sp_sp string ,
        rtg_sp_sp_wt string ,
        rtg_sp_sp_dt string ,
        rtg_sp_long string ,
        rtg_sp_long_wt string ,
        rtg_sp_long_rating_dt string ,
        rtg_sp_shrt string ,
        rtg_sp_shrt_wt string ,
        rtg_sp_shrt_rating_dt string ,
        rtg_sp_issuer_rating string ,
        rtg_sp_issuer_wt string ,
        rtg_sp_issuer_eff_dt string ,
        rtg_fitch string ,
        rtg_fitch_wt string ,
        rtg_fitch_dt string ,
        rtg_fitch_shrt string ,
        rtg_fitch_shrt_wt string ,
        rtg_fitch_shrt_rating_dt string ,
        rtg_fitch_long string ,
        rtg_fitch_long_wt string ,
        rtg_fitch_long_rating_dt string ,
        rtg_fitch_issuer_rating string ,
        rtg_fitch_issuer_wt string ,
        rtg_fitch_issuer_eff_dt string
)
PARTITIONED BY (partition_date string)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  '${DATA_LAKE_SERVER}/santander/bi-corp/staging/aqua/rating_emisiones';