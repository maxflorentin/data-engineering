CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_common.bt_bcaaut_respuestas (
	cod_cue_tarjeta bigint,
	cod_per_nup bigint,
	id_bcaaut_mensaje string,
	cod_bcaaut_resp_novedad string,
	cod_bcaaut_resp_maestro string,
	dt_bcaaut_alta string,
	dt_bcaaut_baja string,
	cod_bcaaut_sigla string,
	dt_bcaaut_oferta string,
	ts_bcaaut_oferta string,
	ds_bcaaut_tipo_telefono string,
	ds_bcaaut_cod_area string,
	ds_bcaaut_nro_telefono string,
	ds_bcaaut_tipodoc string,
	ds_bcaaut_numdoc bigint )
PARTITIONED BY (
    partition_date string)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  '${DATA_LAKE_SERVER}/santander/bi-corp/common/interacciones_canales/bcaaut/fact/bt_bcaaut_respuestas'