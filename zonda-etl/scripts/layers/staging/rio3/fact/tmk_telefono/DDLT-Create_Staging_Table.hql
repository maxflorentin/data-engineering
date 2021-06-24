CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.rio3_tmk_telefono(
id                 	string,
tipo               	string,
ddi                	string,
telediscado        	string,
numero             	string,
interno            	string,
postal             	string,
semaforo           	string,
contacto           	string,
fechaalta          	string,
usuarioalta        	string,
fechamodificacion  	string,
usuariomodificacion	string,
prioridadoutbound  	string,
empresacelular     	string
)
PARTITIONED BY (partition_date string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/staging/rio3/fact/tmk_telefono';