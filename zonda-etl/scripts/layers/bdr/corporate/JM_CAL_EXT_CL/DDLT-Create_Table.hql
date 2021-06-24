create external table if not exists bi_corp_bdr.jm_cal_ext_cl (
r9415_feoperac string,
r9415_s1emp  string,
r9415_idnumcli string,
r9415_cod_agen string,
r9415_ccodplz string,
r9415_tipmoned string,
r9415_feccalif string,
r9415_califmae string
)
partitioned by (`partition_date` string)
stored as parquet
location '${DATA_LAKE_SERVER}/santander/bi-corp/bdr/jm_cal_ext_cl';