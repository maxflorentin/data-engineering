create external table if not exists bi_corp_bdr.jm_linliq_tit(
g4130_feoperac string,
g4130_s1emp  string,
g4130_contra1 string,
g4130_cod_spv string,
g4130_codiglin string,
g4130_lin_liqu string,
g4130_fecultmo string
)
partitioned by (`partition_date` string)
row format serde 'org.apache.hadoop.hive.ql.io.parquet.serde.parquethiveserde'
stored as inputformat 'org.apache.hadoop.hive.ql.io.parquet.mapredparquetinputformat'
outputformat 'org.apache.hadoop.hive.ql.io.parquet.mapredparquetoutputformat'
location '${DATA_LAKE_SERVER}/santander/bi-corp/bdr/jm_linliq_tit';