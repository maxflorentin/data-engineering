create external table if not exists bi_corp_bdr.jm_trz_cliente(
g7015_s1emp  string,
g7015_idnumcli string,
g7015_fecdesde string,
g7015_idsiste string,
g7015_tip_per string,
g7015_cdg_pers string,
g7015_codsiste string,
g7015_fec_has string,
g7015_fec_mod string
)
partitioned by (`partition_date` string)
row format serde 'org.apache.hadoop.hive.ql.io.parquet.serde.parquethiveserde'
stored as inputformat 'org.apache.hadoop.hive.ql.io.parquet.mapredparquetinputformat'
outputformat 'org.apache.hadoop.hive.ql.io.parquet.mapredparquetoutputformat'
location '${DATA_LAKE_SERVER}/santander/bi-corp/bdr/jm_trz_cliente';