create external table if not exists bi_corp_bdr.jm_posic_contr(
e0621_feoperac string,
e0621_s1emp  string,
e0621_contra1 string,
e0621_cta_cont string,
e0621_tip_impt string,
e0621_fec_mes string,
e0621_agrctacb string,
e0621_importh string,
e0621_coddiv string,
e0621_fecultmo string,
e0621_centctbl string,
e0621_ctacgbal string
)
partitioned by (`partition_date` string)
row format serde 'org.apache.hadoop.hive.ql.io.parquet.serde.parquethiveserde'
stored as inputformat 'org.apache.hadoop.hive.ql.io.parquet.mapredparquetinputformat'
outputformat 'org.apache.hadoop.hive.ql.io.parquet.mapredparquetoutputformat'
location '${DATA_LAKE_SERVER}/santander/bi-corp/bdr/jm_posic_contr';