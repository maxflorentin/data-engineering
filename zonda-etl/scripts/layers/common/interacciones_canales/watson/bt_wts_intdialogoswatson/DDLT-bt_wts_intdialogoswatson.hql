CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_common.bt_wts_intdialogoswatson (
    cod_wts_header INT ,
    cod_wts_conversacion INT ,
    ts_wts_conversacion TIMESTAMP ,
    cod_wts_canal STRING ,
    cod_per_nup INT ,
    flag_wts_moratemprana INT ,
    flag_wts_moratardia INT ,
    flag_wts_transferidohumano INT ,
    flag_wts_intencionmora INT ,
    cod_wts_jsessionid STRING ,
    flag_wts_pdfsueldo INT ,
    cod_wts_sesionwatson STRING ,
    ds_wts_pregunta STRING ,
    ds_wts_respuesta STRING ,
    ds_wts_finalintent STRING ,
    ds_wts_intent STRING ,
    fc_wts_confidenceintent DECIMAL(17,4) ,
    flag_wts_respuestautil INT ,
    ds_wts_possiblequestions STRING ,
    ds_wts_suggestiontopics STRING ,
    ds_wts_options STRING ,
    flag_wts_possiblequestions INT ,
    flag_wts_suggestiontopics INT ,
    flag_wts_options INT ,
    ts_wts_pregunta TIMESTAMP ,
    ts_wts_respuesta TIMESTAMP ,
    ds_wts_listintent STRING ,
    ds_wts_finalintentaux1 STRING ,
    fc_wts_confidencefinalintentaux1 DECIMAL(17,4) ,
    ds_wts_finalintentaux2 STRING ,
    fc_wts_confidencefinalintentaux2 DECIMAL(17,4) ,
    ds_wts_telefono STRING ,
    flag_wts_espas INT ,
    cod_wts_gec INT ,
    ds_wts_from1 STRING ,
    flag_wts_intencionderivacion INT ,
    flag_wts_asesordisponible INT ,
    flag_wts_ofreciomail INT ,
    flag_wts_fuerahorario INT )
PARTITIONED BY (
    partition_date string)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  '${DATA_LAKE_SERVER}/santander/bi-corp/common/interacciones_canales/watson/fact/bt_wts_intdialogoswatson' ;