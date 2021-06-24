create external table if not exists bi_corp_bdr.jm_ead_contr(
g5519_feoperac string,
g5519_s1emp  string,
g5519_contra1 string,
g5519_metaplic string,
g5519_fasecalc string,
g5519_mtm  string,
g5519_ead  string,
g5519_espeprov string,
g5519_fecultmo string,
g5519_impnomct string,
g5519_addonbru string,
g5519_addonnet string,
g5519_coefregu string,
g5519_metliqui string,
g5519_mtm_brut string
)
partitioned by (`partition_date` string)
stored as parquet
location '${DATA_LAKE_SERVER}/santander/bi-corp/bdr/jm_ead_contr';
