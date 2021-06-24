CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.tcdt001
(
cdtabla string
,nbtabla string
,rutvalt string
,ident string
,fhalta string
,fhbaja string
,nbcopy string
,numregis string
,longtab string
,indact string
,banmat string
,ofimant string
,bancons string
,oficons string
,ultmod string
,banculmo string
,oficulmo string
,numter string
,usultmod string
,tcmidiom string
,tcidiomas string

)
PARTITIONED BY(partition_date string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/staging/maltc/dim/tcdt001';
