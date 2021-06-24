CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_common.stk_bcaaut_remanentes (
	ts_bcaaut_fecha	string ,
	cod_bcaaut_sigla	string ,
	cod_bcaaut_sucursal	int ,
	ds_bcaaut_sucursal	string ,
	cod_bcaaut_zona	string ,
	ds_bcaaut_operador	string ,
	ds_bcaaut_posiciontipo	string ,
	ds_bcaaut_tipoequipo	string ,
	fc_bcaaut_cargado	decimal(16,2) ,
	fc_bcaaut_dispensado	decimal(16,2) ,
	fc_bcaaut_remanente	decimal(16,2) )
PARTITIONED BY(partition_date string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/common/interacciones_canales/bcaaut/fact/stk_bcaaut_remanentes'