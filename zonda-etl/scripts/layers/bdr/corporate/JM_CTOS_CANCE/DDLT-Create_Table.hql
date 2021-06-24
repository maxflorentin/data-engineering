create external table if not exists bi_corp_bdr.jm_ctos_cance(
h0711_feoperac string,
h0711_s1emp string,
h0711_contra1 string,
h0711_motvcanc string,
h0711_fecultmo string
)
partitioned by (`partition_date` string)
row format serde 'org.apache.hadoop.hive.ql.io.parquet.serde.parquethiveserde'
stored as inputformat 'org.apache.hadoop.hive.ql.io.parquet.mapredparquetinputformat'
outputformat 'org.apache.hadoop.hive.ql.io.parquet.mapredparquetoutputformat'
location '${DATA_LAKE_SERVER}/santander/bi-corp/bdr/jm_ctos_cance';