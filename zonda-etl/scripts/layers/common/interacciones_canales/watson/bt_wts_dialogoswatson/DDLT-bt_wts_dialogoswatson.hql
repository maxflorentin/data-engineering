CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_common.bt_wts_dialogoswatson (
	cod_wts_canal STRING ,
	cod_wts_jsessionid STRING ,
	ts_wts_conversacion TIMESTAMP ,
	cod_wts_emisortipo STRING ,
	ds_wts_emisornombre STRING ,
	ds_wts_message STRING ,
	ds_wts_intent STRING ,
	fc_wts_confidenceintent DECIMAL(17,4) ,
	flag_wts_respuestautil INT ,
	ds_wts_possiblequestions STRING ,
	ds_wts_suggestiontopics STRING ,
	ds_wts_options STRING ,
	cod_per_nup INT ,
	cod_wts_idgenesys STRING ,
	flag_wts_transferidohumano INT ,
	flag_wts_possiblequestions INT ,
	flag_wts_suggestiontopics INT ,
	flag_wts_options INT )
PARTITIONED BY (
    partition_date string)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  '${DATA_LAKE_SERVER}/santander/bi-corp/common/interacciones_canales/watson/fact/bt_wts_dialogoswatson' ;