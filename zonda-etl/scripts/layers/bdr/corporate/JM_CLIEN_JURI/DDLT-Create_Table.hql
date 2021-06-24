create external table if not exists bi_corp_bdr.jm_clien_juri(
g5508_feoperac string,
g5508_s1emp  string,
g5508_idnumcli string,
g5508_inffecha string,
g5508_impfactm string,
g5508_tot_acti string,
g5508_num_empl string,
g5508_orig_fac string,
g5508_orig_act string,
g5508_orig_emp string,
g5508_fecultmo string,
g5508_tdeudacl string,
g5508_rat_cet1 string,
g5508_tasamora string,
g5508_tot_eqty string,
g5508_orgdepen string,
g5508_flgempno string
)
partitioned by (`partition_date` string)
row format serde 'org.apache.hadoop.hive.ql.io.parquet.serde.parquethiveserde'
stored as inputformat 'org.apache.hadoop.hive.ql.io.parquet.mapredparquetinputformat'
outputformat 'org.apache.hadoop.hive.ql.io.parquet.mapredparquetoutputformat'
location '${DATA_LAKE_SERVER}/santander/bi-corp/bdr/jm_clien_juri';