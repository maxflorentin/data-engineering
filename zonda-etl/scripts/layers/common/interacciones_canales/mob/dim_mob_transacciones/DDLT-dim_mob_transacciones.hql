CREATE EXTERNAL TABLE IF NOT EXISTS BI_CORP_COMMON.DIM_MOB_TRANSACCIONES (

COD_MOB_TRANSACCION             STRING,
DS_MOB_DESCRIPCION              STRING,
DS_MOB_MODULO                   STRING,
FLAG_MOB_HABILITADO             INT

)
PARTITIONED BY (PARTITION_DATE STRING)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/common/mob/dim/dim_mob_transacciones'