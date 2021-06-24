CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.comex_rio39_v_formularios(
COD_CONCEP        	STRING,
COD_OPS           	STRING,
SECUENCIA         	STRING,
NRO_FORM          	STRING,
NRO_FORM_REL      	STRING,
TIPO_FORM         	STRING,
NRO_OPER_REL      	STRING,
ESTADO            	STRING,
FECHA_CARGA       	STRING,
FECHA_FIRMA       	STRING,
FECHA_VERIF       	STRING,
TIEMPO_CLIENTE    	STRING,
LVL_RECTIFICACION 	STRING,
NRO_FORM_ORIGINAL 	STRING,
PRIMER_FECHA_CARGA	STRING,
PRIMER_FECHA_FIRMA	STRING
)
PARTITIONED BY (PARTITION_DATE STRING)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/staging/rio39/fact/v_formularios';