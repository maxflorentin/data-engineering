CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.aqua_rating_empresas(
		Unidad_Operativa	 string,
		Ticker	 string,
		MDYS_SU	 string,
		MOODYS_SUW	 string,
		MDYS_SU_DT	 string,
		MDYS_IR	 string,
		MOODYS_I_W	 string,
		MDYS_IR_DT	 string,
		MDYS_LT_FC	 string,
		MOODYS_LFW	 string,
		MDYS_LT_FC_DT	 string,
		MDYS_LT_LC	 string,
		MOODYS_LLW	 string,
		MDYS_LT_LC_DT	 string,
		MDYS_SS	 string,
		MOODYS_SSW	 string,
		MDYS_SS_DT	 string,
		MDYS_SI	 string,
		MOODYS_S_W	 string,
		MDYS_SI_DT	 string,
		MDYS_OUT	 string,
		MDYS_OUT_DT	 string,
		SP_LT_FC	 string,
		SP_LT_F_W	 string,
		SP_LT_FC_DT	 string,
		SP_LT_LC	 string,
		SP_LT_L_W	 string,
		SP_LT_LC_DT	 string,
		SP_OUT	 string,
		SP_OUT_DT	 string,
		FITCH_SU	 string,
		FITCH_SU_W	 string,
		FITCH_SU_DT	 string,
		FITCH_LT_F	 string,
		FITCH_LT_W	 string,
		FITCH_LT_F_DT	 string,
		FITCH_LT_L	 string,
		FITCH_LTLW	 string,
		FITCH_LT_L_DT	 string,
		FITCH_OUT	 string,
		FITCH_OUT_DT	 string,
		SP_ST_LCUR_CREDIT_RT	 string,
		SP_ST_FCUR_CREDIT_RT	 string,
		SP_LT_FCUR_CREDIT_RT	 string,
		SP_LT_LCUR_CREDIT_RT	 string,
		SP_ST_LC_ISR_CR_RT_DT	 string,
		SP_ST_FC_ISR_CR_RT_DT	 string,
		SP_LT_FC_ISR_CR_RT_DT	 string,
		SP_LT_LC_ISR_CR_RT_DT	 string,
		MOODYS_LT_RT	 string,
		MOODYS_LT_DT	 string,
		MOODYS_ST_DEBT_RATING	 string,
		MOODYS_ST_RATING_DATE	 string
)
PARTITIONED BY (partition_date string)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  '${DATA_LAKE_SERVER}/santander/bi-corp/staging/aqua/rating_empresas';