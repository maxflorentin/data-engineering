create external table if not exists bi_corp_bdr.jm_trz_inv_apli(
g7009_s1emp  string,
g7009_idsiste string,
g7009_codsiste string,
g7009_fec_mod string
)
partitioned by (`partition_date` string)
row format serde 'org.apache.hadoop.hive.ql.io.parquet.serde.parquethiveserde'
stored as inputformat 'org.apache.hadoop.hive.ql.io.parquet.mapredparquetinputformat'
outputformat 'org.apache.hadoop.hive.ql.io.parquet.mapredparquetoutputformat'
location '${DATA_LAKE_SERVER}/santander/bi-corp/bdr/jm_trz_inv_apli';