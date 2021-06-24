create external table if not exists bi_corp_bdr.test_jm_vta_carter(
r9736_feoperac  string,
r9736_s1emp   string,
r9736_contra1  string,
r9736_fvtacart  string,
r9736_imppdte  string,
r9736_precioob  string,
r9736_ind_credit string
)
partitioned by (partition_date string)
stored as parquet
location '${DATA_LAKE_SERVER}/santander/bi-corp/bdr/desa/test_jm_vta_carter';