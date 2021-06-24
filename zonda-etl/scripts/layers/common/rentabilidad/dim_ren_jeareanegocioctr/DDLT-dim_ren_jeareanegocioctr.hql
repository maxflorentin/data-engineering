CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_common.dim_ren_jeareanegocioctr (
    cod_ren_pais INT ,
	cod_ren_areanegociopadre STRING ,
	cod_ren_areanegociohijo STRING ,
	ds_ren_areanegociohijo STRING ,
	cod_ren_areanegociojerarquia STRING ,
	flag_ren_consolidacion INT ,
	cod_ren_primercorporativo STRING ,
	cod_ren_userumo STRING ,
	ts_ren_timestumo TIMESTAMP ,
	int_ren_nivel INT ,
	cod_ren_indcorporativo STRING ,
	int_ren_orden INT ,
	partition_date STRING )
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  '${DATA_LAKE_SERVER}/santander/bi-corp/common/rentabilidad/dim/dim_ren_jeareanegocioctr';