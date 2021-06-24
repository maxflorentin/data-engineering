create external table if not exists bi_corp_bdr.jm_gar_posadj(
h0911_s1emp  string,
h0911_contra1 string,
h0911_fecini string,
h0911_tip_gara string,
h0911_biengar1 string,
h0911_fecdesde string,
h0911_fechasta string,
h0911_ingarant string,
h0911_cod_bien string,
h0911_cla_bien string,
h0911_id_pais string,
h0911_inf_regi string,
h0911_nomenti string,
h0911_imgarant string,
h0911_coddiv string,
h0911_fecultmo string
)
partitioned by (`partition_date` string)
row format serde 'org.apache.hadoop.hive.ql.io.parquet.serde.parquethiveserde'
stored as inputformat 'org.apache.hadoop.hive.ql.io.parquet.mapredparquetinputformat'
outputformat 'org.apache.hadoop.hive.ql.io.parquet.mapredparquetoutputformat'
location '${DATA_LAKE_SERVER}/santander/bi-corp/bdr/jm_gar_posadj';
