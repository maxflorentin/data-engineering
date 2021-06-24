create external table if not exists bi_corp_bdr.test_jm_expos_no_con(
e0627_feoperac string,
e0627_s1emp  string,
e0627_contra1 string,
e0627_fec_mes string,
e0627_cta_cont string,
e0627_agrctacb string,
e0627_importh string,
e0627_idctacen string,
e0621_fecultmo string,
e0627_centctbl string,
e0627_entcgbal string,
e0627_ctacgbal string
)
partitioned by (partition_date string)
stored as parquet
location '${DATA_LAKE_SERVER}/santander/bi-corp/bdr/desa/test_jm_expos_no_con';