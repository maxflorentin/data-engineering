create external table if not exists bi_corp_bdr.jm_ead_netting(
e1889_s1emp  string,
e1889_idneting string,
e1889_metaplic string,
e1889_fasecalc string,
e1889_fec_mes string,
e1889_ead  string,
e1889_fecultmo string
)
partitioned by (`partition_date` string)
row format serde 'org.apache.hadoop.hive.ql.io.parquet.serde.parquethiveserde'
stored as inputformat 'org.apache.hadoop.hive.ql.io.parquet.mapredparquetinputformat'
outputformat 'org.apache.hadoop.hive.ql.io.parquet.mapredparquetoutputformat'
location '${DATA_LAKE_SERVER}/santander/bi-corp/bdr/jm_ead_netting';