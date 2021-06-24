create external table if not exists bi_corp_bdr.jm_vta_carter(
r9736_feoperac  string,
r9736_s1emp   string,
r9736_contra1  string,
r9736_fvtacart  string,
r9736_imppdte  string,
r9736_precioob  string,
r9736_ind_credit string
)
partitioned by (`partition_date` string)
row format serde 'org.apache.hadoop.hive.ql.io.parquet.serde.parquethiveserde'
stored as inputformat 'org.apache.hadoop.hive.ql.io.parquet.mapredparquetinputformat'
outputformat 'org.apache.hadoop.hive.ql.io.parquet.mapredparquetoutputformat'
location '${DATA_LAKE_SERVER}/santander/bi-corp/bdr/jm_vta_carter';