create external table if not exists bi_corp_bdr.jm_garant_cto(
g4124_feoperac string,
g4124_s1emp  string,
g4124_contra1 string,
g4124_tip_gara string,
g4124_biengar1 string,
g4124_fecini string,
g4124_fecbaja string,
g4124_fecvcto string,
g4124_cod_gar string,
g4124_cod_gar2 string,
g4124_tipo_ins string,
g4124_ind_pign string,
g4124_tip_aval string,
g4124_tip_cobe string,
g4124_est_gara string,
g4124_porcober string,
g4124_cob_inic string,
g4124_impt_nom string,
g4124_imp_actu string,
g4124_comf_let string,
g4124_fecultmo string,
g4124_num_aseg string,
g4124_coddiv    string,
g4124_idnumcli string,
g4124_indblo string,
g4124_indrgosb string,
g4124_indcobpf string,
g4124_valaseju string,
g4124_repaporc string,
g4124_ordapgar string,
g4124_rangohip string,
g4124_n_impago string
)
partitioned by (`partition_date` string)
row format serde 'org.apache.hadoop.hive.ql.io.parquet.serde.parquethiveserde'
stored as inputformat 'org.apache.hadoop.hive.ql.io.parquet.mapredparquetinputformat'
outputformat 'org.apache.hadoop.hive.ql.io.parquet.mapredparquetoutputformat'
location '${DATA_LAKE_SERVER}/santander/bi-corp/bdr/jm_garant_cto';