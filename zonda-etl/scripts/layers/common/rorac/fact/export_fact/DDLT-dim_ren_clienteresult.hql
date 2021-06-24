CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_common.dim_ren_clienteresult (
    idf_pers_ods	string ,
	cod_per_nup	int ,
	cod_ren_vincula	string ,
	cod_per_tipopersona	string ,
	ds_per_nombre_apellido	string )
PARTITIONED BY(partition_date string)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  '${DATA_LAKE_SERVER}/santander/bi-corp/common/rentabilidad/dim/dim_ren_clienteresult';