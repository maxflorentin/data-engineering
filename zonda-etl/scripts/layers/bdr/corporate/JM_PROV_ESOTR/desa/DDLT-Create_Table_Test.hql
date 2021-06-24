create external table if not exists bi_corp_bdr.test_jm_prov_esotr(
n0625_feoperac string,
n0625_s1emp  string,
n0625_contra1 string,
n0625_tip_impt string,
n0625_importh string,
n0625_coddiv string,
n0625_cta_cont string,
n0625_agrctacb string,
n0625_centctbl string,
n0625_ctacgbal string,
n0625_stage  string
)
partitioned by (partition_date string)
stored as parquet
location '${DATA_LAKE_SERVER}/santander/bi-corp/bdr/desa/test_jm_prov_esotr';