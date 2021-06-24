create external table if not exists bi_corp_bdr.jm_trz_clie_ren(
n0736_s1emp  string,
n0736_idnumcli string,
n0736_emp_ant string,
n0736_clie_ant string,
n0736_motrenu string,
n0736_fealtrel string,
n0736_fec_mod string,
n0736_motrenug string,
n0736_fec_baja string
)
partitioned by (`partition_date` string)
row format serde 'org.apache.hadoop.hive.ql.io.parquet.serde.parquethiveserde'
stored as inputformat 'org.apache.hadoop.hive.ql.io.parquet.mapredparquetinputformat'
outputformat 'org.apache.hadoop.hive.ql.io.parquet.mapredparquetoutputformat'
location '${DATA_LAKE_SERVER}/santander/bi-corp/bdr/jm_trz_clie_ren';