create external table if not exists bi_corp_bdr.jm_derivad_cred(
e1905_s1emp  string,
e1905_contra1 string,
e1905_numepata string,
e1905_idnumcli string,
e1905_fecdesde string,
e1905_fechasta string,
e1905_evenrest string,
e1905_fecultmo string,
e1905_fecruptr string
)
partitioned by (`partition_date` string)
row format serde 'org.apache.hadoop.hive.ql.io.parquet.serde.parquethiveserde'
stored as inputformat 'org.apache.hadoop.hive.ql.io.parquet.mapredparquetinputformat'
outputformat 'org.apache.hadoop.hive.ql.io.parquet.mapredparquetoutputformat'
location '${DATA_LAKE_SERVER}/santander/bi-corp/bdr/jm_derivad_cred';