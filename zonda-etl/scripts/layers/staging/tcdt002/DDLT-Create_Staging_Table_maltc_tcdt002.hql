CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.tcdt002
(
cdtabla string
,cdcampo string
,poscamp string
,nuclave string
,tabval string
,ultmod string
,banculmo string
,oficulmo string
,numter string
,usultmod string
,idf_indice string
,idf_duplic string
)
PARTITIONED BY(partition_date string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/staging/maltc/dim/tcdt002';
