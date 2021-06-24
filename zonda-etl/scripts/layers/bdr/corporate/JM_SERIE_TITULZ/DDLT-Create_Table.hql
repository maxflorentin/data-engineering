create external table if not exists bi_corp_bdr.jm_serie_titulz(
e0635_s1emp  string,
e0635_tram_tit string,
e0635_cod_spv string,
e0635_importh string,
e0635_cod_prel string,
e0635_agen_rat string,
e0635_fchultac string,
e0635_rati_ext string,
e0635_ratingp string,
e0635_fecultmo string
)
partitioned by (`partition_date` string)
row format serde 'org.apache.hadoop.hive.ql.io.parquet.serde.parquethiveserde'
stored as inputformat 'org.apache.hadoop.hive.ql.io.parquet.mapredparquetinputformat'
outputformat 'org.apache.hadoop.hive.ql.io.parquet.mapredparquetoutputformat'
location '${DATA_LAKE_SERVER}/santander/bi-corp/bdr/jm_serie_titulz';