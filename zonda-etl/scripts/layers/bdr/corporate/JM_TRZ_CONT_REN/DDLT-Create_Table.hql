create external table if not exists bi_corp_bdr.jm_trz_cont_ren (
g7025_s1emp     string,
g7025_contra1   string,
g7025_emp_ant   string,
g7025_cont_ant  string,
g7025_motrenu   string,
g7025_fealtrel  string,
g7025_fec_mod   string,
g7025_imprestr string,
g7025_coddiv string,
g7025_motrenug string,
g7025_fec_baja string
)
partitioned by (`partition_date` string)
row format serde 'org.apache.hadoop.hive.ql.io.parquet.serde.parquethiveserde'
stored as inputformat 'org.apache.hadoop.hive.ql.io.parquet.mapredparquetinputformat'
outputformat 'org.apache.hadoop.hive.ql.io.parquet.mapredparquetoutputformat'
location '${DATA_LAKE_SERVER}/santander/bi-corp/bdr/jm_trz_cont_ren';