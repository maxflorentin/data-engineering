create external table if not exists bi_corp_bdr.jm_estado_vig(
r8182_feoperac string,
r8182_s1emp  string,
r8182_idnumcli string,
r8182_codorvis string,
r8182_fecestad string,
r8182_fecgrado string,
r8182_est_feve string,
r8182_gra_feve string,
r8182_feve_pol string,
r8182_alertsar string,
r8182_fecultmo string
)
partitioned by (`partition_date` string)
row format serde 'org.apache.hadoop.hive.ql.io.parquet.serde.parquethiveserde'
stored as inputformat 'org.apache.hadoop.hive.ql.io.parquet.mapredparquetinputformat'
outputformat 'org.apache.hadoop.hive.ql.io.parquet.mapredparquetoutputformat'
location '${DATA_LAKE_SERVER}/santander/bi-corp/bdr/jm_estado_vig';