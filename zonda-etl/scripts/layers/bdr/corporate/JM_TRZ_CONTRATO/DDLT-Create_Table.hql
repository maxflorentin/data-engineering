create external table if not exists bi_corp_bdr.jm_trz_contrato (
g7014_s1emp  string,
g7014_contra1 string,
g7014_fecdesde string,
g7014_idsiste string,
g7014_codsiste string,
g7014_fec_has string,
g7014_fec_mod string,
g7014_ind_titu string
)
partitioned by (`partition_date` string)
row format serde 'org.apache.hadoop.hive.ql.io.parquet.serde.parquethiveserde'
stored as inputformat 'org.apache.hadoop.hive.ql.io.parquet.mapredparquetinputformat'
outputformat 'org.apache.hadoop.hive.ql.io.parquet.mapredparquetoutputformat'
location '${DATA_LAKE_SERVER}/santander/bi-corp/bdr/jm_trz_contrato';