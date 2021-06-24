create external table if not exists bi_corp_bdr.jm_inm_posadj(
o9220_feoperac string,
o9220_s1emp    string,
o9220_idinmueb string,
o9220_id_ue    string,
o9220_ref_cata string,
o9220_dsdirec  string,
o9220_cd_post  string,
o9220_id_pais  string,
o9220_tip_acti string,
o9220_stip_act string,
o9220_tip_suel string,
o9220_sit_cons string,
o9220_idnumcli string,
o9220_fecadjud string,
o9220_oriadjud string,
o9220_deudabr  string,
o9220_provacad string,
o9220_provdtac string,
o9220_gastosac string,
o9220_gastosna string,
o9220_valacmom string,
o9220_valacadj string,
o9220_fecultas string,
o9220_ind_esta string,
o9220_prec_alq string,
o9220_fec_vent string,
o9220_pre_vent string,
o9220_cos_vent string,
o9220_ent_valo string,
o9220_coddiv   string,
o9220_enfq_sol string
)
partitioned by (`partition_date` string)
row format serde 'org.apache.hadoop.hive.ql.io.parquet.serde.parquethiveserde'
stored as inputformat 'org.apache.hadoop.hive.ql.io.parquet.mapredparquetinputformat'
outputformat 'org.apache.hadoop.hive.ql.io.parquet.mapredparquetoutputformat'
location '${DATA_LAKE_SERVER}/santander/bi-corp/bdr/jm_inm_posadj';