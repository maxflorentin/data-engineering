create external table if not exists bi_corp_bdr.jm_interv_cto(
g4128_feoperac string,
g4128_s1emp  string,
g4128_contra1 string,
g4128_tipintev string,
g4128_tipintv2 string,
g4128_numordin string,
g4128_idnumcli string,
g4128_formintv string,
g4128_porpartn string,
g4128_fecultmo string
)
partitioned by (`partition_date` string)
row format serde 'org.apache.hadoop.hive.ql.io.parquet.serde.parquethiveserde'
stored as inputformat 'org.apache.hadoop.hive.ql.io.parquet.mapredparquetinputformat'
outputformat 'org.apache.hadoop.hive.ql.io.parquet.mapredparquetoutputformat'
location '${DATA_LAKE_SERVER}/santander/bi-corp/bdr/jm_interv_cto';