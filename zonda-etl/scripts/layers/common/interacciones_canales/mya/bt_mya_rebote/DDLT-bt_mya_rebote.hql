CREATE EXTERNAL TABLE IF NOT EXISTS BI_CORP_COMMON.BT_MYA_REBOTE (
ID_MYA_REBOTE                           STRING,
COD_MYA_DESTINO                         STRING,
TS_MYA_FECHA                            STRING,
COD_MYA_RAZON                           STRING,
DS_MYA_RAZON                            STRING,
COD_MYA_TIPO                            STRING,
DS_MYA_TIPO                             STRING

)
PARTITIONED BY (PARTITION_DATE STRING)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/common/mya/fact/bt_mya_rebote'