create external table if not exists bi_corp_bdr.jm_adic_calco(
n0632_feoperac string,
n0632_s1emp  string,
n0632_contra1 string,
n0632_fecforz string,
n0632_motivfor string,
n0632_fecaprob string,
n0632_orgaprob string,
n0632_fecultmo string,
n0632_vincsco string
)
partitioned by (`partition_date` string)
row format serde 'org.apache.hadoop.hive.ql.io.parquet.serde.parquethiveserde'
stored as inputformat 'org.apache.hadoop.hive.ql.io.parquet.mapredparquetinputformat'
outputformat 'org.apache.hadoop.hive.ql.io.parquet.mapredparquetoutputformat'
location '${DATA_LAKE_SERVER}/santander/bi-corp/bdr/jm_adic_calco';