create external table if not exists bi_corp_bdr.test_jm_grup_rela(
g5515_feoperac string,
g5515_s1emp  string,
g5515_grup_eco string,
g5515_idnumcli string,
g5515_rol_jera string,
g5515_fecultmo string
)
partitioned by (partition_date string)
stored as parquet
location '${DATA_LAKE_SERVER}/santander/bi-corp/bdr/desa/test_jm_grup_rela';