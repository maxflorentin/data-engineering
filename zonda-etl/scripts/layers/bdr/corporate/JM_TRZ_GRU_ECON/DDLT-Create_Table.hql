create external table if not exists bi_corp_bdr.jm_trz_gru_econ (
g7017_s1emp  string,
g7017_grup_eco string,
g7017_fecdesde string,
g7017_idsiste string,
g7017_codsiste string,
g7017_fec_has string,
g7017_fec_mod string
)
partitioned by (`partition_date` string)
row format serde 'org.apache.hadoop.hive.ql.io.parquet.serde.parquethiveserde'
stored as inputformat 'org.apache.hadoop.hive.ql.io.parquet.mapredparquetinputformat'
outputformat 'org.apache.hadoop.hive.ql.io.parquet.mapredparquetoutputformat'
location '${DATA_LAKE_SERVER}/santander/bi-corp/bdr/jm_trz_gru_econ';