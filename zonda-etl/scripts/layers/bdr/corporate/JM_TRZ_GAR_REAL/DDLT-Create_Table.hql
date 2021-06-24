create external table if not exists bi_corp_bdr.jm_trz_gar_real (
g7018_s1emp  string,
g7018_biengar1 string,
g7018_fecdesde string,
g7018_idsiste string,
g7018_codsiste string,
g7018_fec_has string,
g7018_fec_mod string
)
partitioned by (`partition_date` string)
row format serde 'org.apache.hadoop.hive.ql.io.parquet.serde.parquethiveserde'
stored as inputformat 'org.apache.hadoop.hive.ql.io.parquet.mapredparquetinputformat'
outputformat 'org.apache.hadoop.hive.ql.io.parquet.mapredparquetoutputformat'
location '${DATA_LAKE_SERVER}/santander/bi-corp/bdr/jm_trz_gar_real';