create external table if not exists bi_corp_bdr.jm_val_inmu(
o9218_feoperac string,
o9218_s1emp  string,
o9218_idinmueb string,
o9218_fechvalr string,
o9218_tipvalim string,
o9218_imp_inmu string,
o9218_coddiv string,
o9218_fecultmo string
)
partitioned by (`partition_date` string)
row format serde 'org.apache.hadoop.hive.ql.io.parquet.serde.parquethiveserde'
stored as inputformat 'org.apache.hadoop.hive.ql.io.parquet.mapredparquetinputformat'
outputformat 'org.apache.hadoop.hive.ql.io.parquet.mapredparquetoutputformat'
location '${DATA_LAKE_SERVER}/santander/bi-corp/bdr/jm_val_inmu';
