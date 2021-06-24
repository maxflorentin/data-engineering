create external table if not exists bi_corp_bdr.jm_flujos(
r9746_feoperac string,
r9746_s1emp  string,
r9746_contra1 string,
r9746_fecmvto string,
r9746_clasmvto string,
r9746_importe string,
r9746_salonbal string
)
partitioned by (`partition_date` string)
row format serde 'org.apache.hadoop.hive.ql.io.parquet.serde.parquethiveserde'
stored as inputformat 'org.apache.hadoop.hive.ql.io.parquet.mapredparquetinputformat'
outputformat 'org.apache.hadoop.hive.ql.io.parquet.mapredparquetoutputformat'
location '${DATA_LAKE_SERVER}/santander/bi-corp/bdr/jm_flujos';