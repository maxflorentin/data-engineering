create external table if not exists bi_corp_bdr.jm_inf_ad_cli(
h0780_feoperac string,
h0780_s1emp  string,
h0780_idnumcli string,
h0780_tipinfrl string,
h0780_tipinfrg string,
h0780_importh string,
h0780_coddiv string,
h0780_fechaact string,
h0780_fecultmo string,
h0780_cuotpres string,
h0780_ingclien string,
h0780_num_mtos string,
h0780_fecingre string,
h0780_rdeuding string,
h0780_tip_empr string,
h0780_tot_ingr string,
h0780_tot_deuf string,
h0780_flgacsld string
)
partitioned by (`partition_date` string)
row format serde 'org.apache.hadoop.hive.ql.io.parquet.serde.parquethiveserde'
stored as inputformat 'org.apache.hadoop.hive.ql.io.parquet.mapredparquetinputformat'
outputformat 'org.apache.hadoop.hive.ql.io.parquet.mapredparquetoutputformat'
location '${DATA_LAKE_SERVER}/santander/bi-corp/bdr/jm_inf_ad_cli';