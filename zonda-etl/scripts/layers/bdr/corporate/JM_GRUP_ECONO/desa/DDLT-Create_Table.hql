create external table if not exists bi_corp_bdr.test_jm_grup_econo(
g5512_feoperac string,
g5512_s1emp  string,
g5512_grup_eco string,
g5512_dcodgrup string,
g5512_dgrupo string,
g5512_idsucur string,
g5512_id_pais string,
g5512_impfactm string,
g5512_tot_acti string,
g5512_num_empl string,
g5512_orig_fac string,
g5512_orig_act string,
g5512_orig_emp string,
g5512_impt_rgo string,
g5512_fecinfac string,
g5512_grecosec string,
g5512_coddiv string,
g5512_fecultmo string,
g5512_tdeudagr string,
g5512_flgempno string
)
partitioned by (partition_date string)
stored as parquet
location '${DATA_LAKE_SERVER}/santander/bi-corp/bdr/desa/test_jm_grup_econo';