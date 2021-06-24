create external table if not exists bi_corp_bdr.jm_cal_in_co (
e9952_feoperac string,
e9952_s1emp  string,
e9952_contra1 string,
e9952_feccali string,
e9952_tipmodel string,
e9952_tipmode2 string,
e9952_idmodel string,
e9952_tipo  string,
e9952_idpunsco string,
e9952_feccaduc string,
e9952_c1spid string,
e9952_c1digcon string,
e9952_c1resp string,
e9952_c1circ string,
e9952_fecultmo string,
e9952_c1motexc string,
e9952_tipmode3 string,
e9952_scovinpr string,
e9952_fecinofc string
)
partitioned by (`partition_date` string)
row format serde 'org.apache.hadoop.hive.ql.io.parquet.serde.parquethiveserde'
stored as inputformat 'org.apache.hadoop.hive.ql.io.parquet.mapredparquetinputformat'
outputformat 'org.apache.hadoop.hive.ql.io.parquet.mapredparquetoutputformat'
location '${DATA_LAKE_SERVER}/santander/bi-corp/bdr/jm_cal_in_co';