create external table if not exists bi_corp_bdr.jm_tituliz(
g4132_feoperac string,
g4132_s1emp  string,
g4132_contra1 string,
g4132_cod_spv string,
g4132_tram_tit string,
g4132_tporcent string,
g4132_tporcen string,
g4132_impsldc string,
g4132_impsld string,
g4132_fecha  string,
g4132_indcump string,
g4132_fecultmo string,
g4132_tituporc string,
g4132_tacttitu string,
g4132_impvsyac string,
g4132_imintres string,
g4132_cta_cont string,
g4132_agrctacb string,
g4132_ctactbsi string,
g4132_agctcbsi string
)
partitioned by (`partition_date` string)
row format serde 'org.apache.hadoop.hive.ql.io.parquet.serde.parquethiveserde'
stored as inputformat 'org.apache.hadoop.hive.ql.io.parquet.mapredparquetinputformat'
outputformat 'org.apache.hadoop.hive.ql.io.parquet.mapredparquetoutputformat'
location '${DATA_LAKE_SERVER}/santander/bi-corp/bdr/jm_tituliz';