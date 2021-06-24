create external table if not exists bi_corp_bdr.jm_rel_bargl_lo(
n0741_s1emp  string,
n0741_codnego string,
n0741_codbarg string,
n0741_codbarle string,
n0741_fecdesde string,
n0741_fechasta string,
n0741_fecgrab string,
n0741_fecultmo string,
n0741_codnegol string
)
partitioned by (`partition_date` string)
row format serde 'org.apache.hadoop.hive.ql.io.parquet.serde.parquethiveserde'
stored as inputformat 'org.apache.hadoop.hive.ql.io.parquet.mapredparquetinputformat'
outputformat 'org.apache.hadoop.hive.ql.io.parquet.mapredparquetoutputformat'
location '${DATA_LAKE_SERVER}/santander/bi-corp/bdr/jm_rel_bargl_lo';