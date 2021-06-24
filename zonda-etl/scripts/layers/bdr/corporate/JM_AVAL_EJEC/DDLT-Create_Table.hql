create external table if not exists bi_corp_bdr.jm_aval_ejec (
g7232_s1emp  string,
g7232_contra1 string,
g7232_fecejec string,
g7232_impejecu string,
g7232_coddiv  string,
g7232_imprgviv string,
g7232_fecultmo string
)
partitioned by (`partition_date` string)
row format serde 'org.apache.hadoop.hive.ql.io.parquet.serde.parquethiveserde'
stored as inputformat 'org.apache.hadoop.hive.ql.io.parquet.mapredparquetinputformat'
outputformat 'org.apache.hadoop.hive.ql.io.parquet.mapredparquetoutputformat'
location '${DATA_LAKE_SERVER}/santander/bi-corp/bdr/jm_aval_ejec';