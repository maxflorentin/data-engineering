CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_common.dim_ren_genericresult (
    cod_ren_contrato	string ,
	cod_ren_contenido	string ,
	cod_ren_pais	string ,
	dt_ren_altacontrato	string ,
	dt_ren_vencontrato	string ,
	dt_ren_reestruccontrato	string ,
	cod_ren_producto_generic	string ,
	cod_ren_subproducto_generic	string )
PARTITIONED BY(partition_date string)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  '${DATA_LAKE_SERVER}/santander/bi-corp/common/rentabilidad/dim/dim_ren_genericresult' ;