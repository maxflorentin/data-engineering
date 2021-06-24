create external table if not exists bi_corp_bdr.jm_inmuebles(
o9270_feoperac string,
o9270_s1emp  string,
o9270_idinmueb string,
o9270_tipoutil string,
o9270_tam_inmu string,
o9270_ano_cons string,
o9270_met_cons string,
o9270_cozona string,
o9270_tip_ppda string,
o9270_rrventaf string,
o9270_rrventap string,
o9270_mrventa string,
o9270_rrajvtaf string,
o9270_rrajvtap string,
o9270_mrajvta string,
o9270_ptventaf string,
o9270_ptventap string,
o9270_pptventa string,
o9270_id_metro string,
o9270_id_ciest string,
o9270_poderadq string
)
partitioned by (`partition_date` string)
row format serde 'org.apache.hadoop.hive.ql.io.parquet.serde.parquethiveserde'
stored as inputformat 'org.apache.hadoop.hive.ql.io.parquet.mapredparquetinputformat'
outputformat 'org.apache.hadoop.hive.ql.io.parquet.mapredparquetoutputformat'
location '${DATA_LAKE_SERVER}/santander/bi-corp/bdr/jm_inmuebles';