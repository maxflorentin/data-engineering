create external table if not exists bi_corp_bdr.jm_baremos_loc (
r8180_s1emp  string,
r8180_codnegol string,
r8180_codbarle string,
r8180_fecdesde string,
r8180_fechasta string,
r8180_descbarl string,
r8180_fecultmo string
)
partitioned by (`partition_date` string)
row format serde 'org.apache.hadoop.hive.ql.io.parquet.serde.parquethiveserde'
stored as inputformat 'org.apache.hadoop.hive.ql.io.parquet.mapredparquetinputformat'
outputformat 'org.apache.hadoop.hive.ql.io.parquet.mapredparquetoutputformat'
location '${DATA_LAKE_SERVER}/santander/bi-corp/bdr/jm_baremos_loc';