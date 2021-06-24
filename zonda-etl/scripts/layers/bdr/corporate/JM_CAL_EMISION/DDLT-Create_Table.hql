create external table if not exists bi_corp_bdr.jm_cal_emision (
e0665_s1emp  string,
e0665_id_emisi string,
e0665_cod_emis string,
e0665_cod_agen string,
e0665_ccodplz string,
e0665_feccali string,
e0665_codmercd string,
e0665_fechasta string,
e0665_califmae string,
e0665_fecultmo string
)
partitioned by (`partition_date` string)
row format serde 'org.apache.hadoop.hive.ql.io.parquet.serde.parquethiveserde'
stored as inputformat 'org.apache.hadoop.hive.ql.io.parquet.mapredparquetinputformat'
outputformat 'org.apache.hadoop.hive.ql.io.parquet.mapredparquetoutputformat'
location '${DATA_LAKE_SERVER}/santander/bi-corp/bdr/jm_cal_emision';