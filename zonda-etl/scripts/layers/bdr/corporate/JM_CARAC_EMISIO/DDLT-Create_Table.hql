create external table if not exists bi_corp_bdr.jm_carac_emisio(
e1907_s1emp  string,
e1907_idemisi string,
e1907_idemis string,
e1907_fecdesde string,
e1907_fechasta string,
e1907_idnumcli string,
e1907_coddiv string,
e1907_fecvento string,
e1907_dsubor string,
e1907_deudpubl string,
e1907_fecultmo string
)
partitioned by (`partition_date` string)
row format serde 'org.apache.hadoop.hive.ql.io.parquet.serde.parquethiveserde'
stored as inputformat 'org.apache.hadoop.hive.ql.io.parquet.mapredparquetinputformat'
outputformat 'org.apache.hadoop.hive.ql.io.parquet.mapredparquetoutputformat'
location '${DATA_LAKE_SERVER}/santander/bi-corp/bdr/jm_carac_emisio';