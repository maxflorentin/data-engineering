create external table if not exists bi_corp_bdr.test_jm_cal_in_cl (
e9954_feoperac string,
e9954_s1emp  string,
e9954_idnumcli string,
e9954_feccali string,
e9954_tipmodel string,
e9954_tipmode2 string,
e9954_idmodel string,
e9954_tipo  string,
e9954_idpunsco string,
e9954_feccaduc string,
e9954_c1tarpun string,
e9954_c1spid string,
e9954_c1digcon string,
e0621_fecultmo string,
e9954_motivfor string,
e9954_idpunsc2 string,
e9954_fecrepfi string,
e9954_fecinofc string
)
partitioned by (partition_date string)
stored as parquet
location '${DATA_LAKE_SERVER}/santander/bi-corp/bdr/desa/test_jm_cal_in_cl';