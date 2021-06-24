create external table if not exists bi_corp_bdr.jm_rel_inmgar(
o9217_s1emp  string,
o9217_idinmueb string,
o9217_biengar1 string,
o9217_fecdesde string,
o9217_fechasta string
)
partitioned by (`partition_date` string)
row format serde 'org.apache.hadoop.hive.ql.io.parquet.serde.parquethiveserde'
stored as inputformat 'org.apache.hadoop.hive.ql.io.parquet.mapredparquetinputformat'
outputformat 'org.apache.hadoop.hive.ql.io.parquet.mapredparquetoutputformat'
location '${DATA_LAKE_SERVER}/santander/bi-corp/bdr/jm_rel_inmgar';