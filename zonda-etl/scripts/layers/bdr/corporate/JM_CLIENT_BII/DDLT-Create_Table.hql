create external table if not exists bi_corp_bdr.jm_client_bii(
g4093_feoperac string,
g4093_s1emp  string,
g4093_idnumcli string,
g4093_tip_pers string,
g4093_apnomper string,
g4093_datexto1 string,
g4093_datexto2 string,
g4093_identper string,
g4093_codidper string,
g4093_clie_glo string,
g4093_idsucur string,
g4093_carter string,
g4093_id_pais string,
g4093_cod_sect string,
g4093_cod_sec2 string,
g4093_cod_sec3 string,
g4093_clisegm string,
g4093_clisegl1 string,
g4093_tipsegl1 string,
g4093_clisegl2 string,
g4093_tipsegl2 string,
g4093_fchini string,
g4093_fchfin string,
g4093_fecultmo string,
g4093_industry string,
g4093_exclcli string,
g4093_indbcart string,
g4093_fecnacim string,
g4093_paisneg string,
g4093_cdpostal string,
g4093_tto_espe string,
g4093_gra_vinc string,
g4093_utp_cli string,
g4093_finiutcl string,
g4093_ffinutcl string
)
partitioned by (`partition_date` string)
row format serde 'org.apache.hadoop.hive.ql.io.parquet.serde.parquethiveserde'
stored as inputformat 'org.apache.hadoop.hive.ql.io.parquet.mapredparquetinputformat'
outputformat 'org.apache.hadoop.hive.ql.io.parquet.mapredparquetoutputformat'
location '${DATA_LAKE_SERVER}/santander/bi-corp/bdr/jm_client_bii';