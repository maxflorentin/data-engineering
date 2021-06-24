create external table if not exists bi_corp_bdr.jm_val_gara(
g4134_feoperac string,
g4134_s1emp  string,
g4134_biengar1 string,
g4134_fechvalr string,
g4134_nomenti string,
g4134_imgarant string,
g4134_fecultmo string,
g4134_tipvaln string,
g4134_tip_gara string,
g4134_coddiv string
)
partitioned by (`partition_date` string)
row format serde 'org.apache.hadoop.hive.ql.io.parquet.serde.parquethiveserde'
stored as inputformat 'org.apache.hadoop.hive.ql.io.parquet.mapredparquetinputformat'
outputformat 'org.apache.hadoop.hive.ql.io.parquet.mapredparquetoutputformat'
location '${DATA_LAKE_SERVER}/santander/bi-corp/bdr/jm_val_gara';