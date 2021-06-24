create external table if not exists bi_corp_bdr.jm_trz_grec_ren(
o2825_s1emp  string,
o2825_grup_eco string,
o2825_emp_ant string,
o2825_grup_ant string,
o2825_motrenu string,
o2825_fealtrel string,
o2825_fec_mod string,
o2825_motrenug string,
o2825_fec_baja string
)
partitioned by (`partition_date` string)
row format serde 'org.apache.hadoop.hive.ql.io.parquet.serde.parquethiveserde'
stored as inputformat 'org.apache.hadoop.hive.ql.io.parquet.mapredparquetinputformat'
outputformat 'org.apache.hadoop.hive.ql.io.parquet.mapredparquetoutputformat'
location '${DATA_LAKE_SERVER}/santander/bi-corp/bdr/jm_trz_grec_ren';