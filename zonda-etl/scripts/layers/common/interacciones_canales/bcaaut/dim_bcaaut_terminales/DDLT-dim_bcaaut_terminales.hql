CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_common.dim_bcaaut_terminales (
	cod_bcaaut_sigla string,
	ds_bcaaut_serie string,
	ds_bcaaut_tipoequipo string,
	ds_bcaaut_modelo string,
	ds_bcaaut_marca string,
	ds_bcaaut_apertura string,
	flag_bcaaut_reconocebilletes int,
	ds_bcaaut_funcionalidad string,
	cod_bcaaut_sucursal string,
	ds_bcaaut_sucursal string,
	cod_bcaaut_sucursalcc string,
	cod_bcaaut_zona string,
	ds_bcaaut_zonal string,
	ds_bcaaut_sucursalcalle string,
	ds_bcaaut_sucursalnro string,
	ds_bcaaut_sucursalpostal string,
	ds_bcaaut_sucursalpostalcpa string,
	ds_bcaaut_sucursaltelefono string,
	ds_bcaaut_sucursallocalidad string,
	ds_bcaaut_sucursalprovincia string,
	ds_bcaaut_tiposucursal string,
	ds_bcaaut_operador string,
	ds_bcaaut_red string )
PARTITIONED BY (partition_date string)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  '${DATA_LAKE_SERVER}/santander/bi-corp/common/interacciones_canales/bcaaut/dim/dim_bcaaut_terminales' ;