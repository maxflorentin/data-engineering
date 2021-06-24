CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_common.dim_ren_productosctrco (
    cod_ren_pais INT ,
    cod_ren_producto STRING ,
	cod_ren_producto_niv_1 STRING ,
	cod_ren_producto_niv_2 STRING ,
	cod_ren_producto_niv_3 STRING ,
	cod_ren_producto_niv_4 STRING ,
	cod_ren_producto_niv_5 STRING ,
	cod_ren_producto_niv_6 STRING ,
	cod_ren_producto_niv_7 STRING ,
	cod_ren_producto_niv_8 STRING ,
	cod_ren_producto_niv_9 STRING ,
	cod_ren_producto_niv_10 STRING ,
	cod_ren_producto_niv_11 STRING ,
	cod_ren_producto_niv_12 STRING ,
	cod_ren_producto_niv_13 STRING ,
	cod_ren_producto_niv_14 STRING ,
	ds_ren_producto_niv_1 STRING ,
	ds_ren_producto_niv_2 STRING ,
	ds_ren_producto_niv_3 STRING ,
	ds_ren_producto_niv_4 STRING ,
	ds_ren_producto_niv_5 STRING ,
	ds_ren_producto_niv_6 STRING ,
	ds_ren_producto_niv_7 STRING ,
	ds_ren_producto_niv_8 STRING ,
	ds_ren_producto_niv_9 STRING ,
	ds_ren_producto_niv_10 STRING ,
	ds_ren_producto_niv_11 STRING ,
	ds_ren_producto_niv_12 STRING ,
	ds_ren_producto_niv_13 STRING ,
	ds_ren_producto_niv_14 STRING ,
	ds_ren_producto_signo STRING ,
	partition_date STRING )
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  '${DATA_LAKE_SERVER}/santander/bi-corp/common/rentabilidad/dim/dim_ren_productosctrco';