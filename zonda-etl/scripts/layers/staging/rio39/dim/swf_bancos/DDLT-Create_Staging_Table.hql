CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.comex_rio39_swf_bancos (
BIC_CODE                   STRING,
BRANCH_CODE                STRING,
NAME1                      STRING,
NAME2                      STRING,
NAME3                      STRING,
BRANCH_INFORMATION1        STRING,
BRANCH_INFORMATION2        STRING,
CITY_HEADING               STRING,
SUBTYPE_INDICATION         STRING,
DIREC_FISICA1              STRING,
DIREC_FISICA2              STRING,
DIREC_FISICA3              STRING,
DIREC_FISICA4              STRING,
LOCATION1                  STRING,
LOCATION2                  STRING,
LOCATION3                  STRING,
NOM_PAIS1                  STRING,
NOM_PAIS2                  STRING,
POB_CODPOSTAL              STRING,
POB_LOCATION1              STRING,
POB_LOCATION2              STRING,
POB_LOCATION3              STRING,
POB_NOM_PAIS1              STRING,
POB_NOM_PAIS2              STRING,
COD_BCRA                   STRING,
COD_BANCO                  STRING,
MODO_OPERACION             STRING,
CODIGOBANCOPAIS            STRING,
USU_BAJA                   STRING,
FECHA_BAJA                 STRING,
USU_REACTIVA               STRING,
FECHA_REACTIVA             STRING,
USU_ALTA                   STRING,
FECHA_ALTA                 STRING,
USU_MODIF                  STRING,
FECHA_MODIF                STRING,
BCO_SEGUIMIENTO            STRING
)
PARTITIONED BY (PARTITION_DATE string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/staging/rio39/dim/swf_bancos';