create external table if not exists bi_corp_bdr.jm_deud_ini_bdi(
e0687_s1emp  string,
e0687_contra1 string,
e0687_fecini string,
e0687_bdsitu string,
e0687_fecsitu string,
e0687_impvalor string,
e0687_fecultmo string
)
partitioned by (`partition_date` string)
row format serde 'org.apache.hadoop.hive.ql.io.parquet.serde.parquethiveserde'
stored as inputformat 'org.apache.hadoop.hive.ql.io.parquet.mapredparquetinputformat'
outputformat 'org.apache.hadoop.hive.ql.io.parquet.mapredparquetoutputformat'
location '${DATA_LAKE_SERVER}/santander/bi-corp/bdr/jm_deud_ini_bdi';