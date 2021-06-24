CREATE TABLE bi_corp_staging.rosetta_controles
( fecha_proceso STRING,
cod_incidencia STRING ,
num_reg_total STRING ,
num_reg_error STRING ,
op_timestamp STRING  )
PARTITIONED BY ( 	 tabla string)
	ROW FORMAT SERDE
	  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
	WITH SERDEPROPERTIES (
	  'field.delim'='|',
	  'serialization.format'='|')
	STORED AS INPUTFORMAT
	  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
	OUTPUTFORMAT
	  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
	LOCATION	  'hdfs://namesrvprod/santander/bi-corp/staging/rosetta_controles';