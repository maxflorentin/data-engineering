CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.aqua_rating_paises(
        codigo_iso_pais string,
		mdys_lt_fc string ,
        moodys_lfw string ,
        mdys_lt_fc_dt string ,
        mdys_out string ,
        mdys_out_dt string ,
        sp_lt_fc string ,
        sp_lt_f_w string ,
        sp_lt_fc_dt string ,
        sp_lt_lc string ,
        sp_lt_l_w string ,
        sp_lt_lc_dt string ,
        sp_out string ,
        sp_out_dt string ,
        fitch_lt_f string ,
        fitch_lt_w string ,
        fitch_lt_f_dt string ,
        fitch_out string ,
        fitch_out_dt string ,
        rating_interno string ,
        fecha_rating_interno string ,
        sp_st_local_currency_issuer_credit_rating string ,
        sp_st_foreign_currency_issuer_credit_rating string ,
        sp_st_lc_issuer_credit_rating_date string ,
        sp_st_fc_issuer_credit_rating_date string ,
        moodys_local_crny_issuer_rating string ,
        rtg_mdy_lt_lc_debt_rating string ,
        rtg_mdy_lt_lc_debt_rtg_dt string ,
        rtg_mdy_st_fc_debt string ,
        rtg_mdy_st_fc_rtg_dt string ,
        rtg_mdy_st_lc_debt string ,
        rtg_mdy_st_lc_rtg_dt string 
)
PARTITIONED BY (partition_date string)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  '${DATA_LAKE_SERVER}/santander/bi-corp/staging/aqua/rating_paises';
