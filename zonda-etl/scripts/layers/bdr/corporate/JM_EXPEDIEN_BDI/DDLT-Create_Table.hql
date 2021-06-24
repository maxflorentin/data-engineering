create external table if not exists bi_corp_bdr.jm_expedien_bdi(
e0685_s1emp string,
e0685_contra1 string,
e0685_fecini string,
e0685_fecfin string,
e0685_idsucur string,
e0685_id_prod string,
e0685_idpro_lc string,
e0685_bdsitu string,
e0685_bdsitusa string,
e0685_caus_cie string,
e0685_fecultmo string
)
partitioned by (`partition_date` string)
row format serde 'org.apache.hadoop.hive.ql.io.parquet.serde.parquethiveserde'
stored as inputformat 'org.apache.hadoop.hive.ql.io.parquet.mapredparquetinputformat'
outputformat 'org.apache.hadoop.hive.ql.io.parquet.mapredparquetoutputformat'
location '${DATA_LAKE_SERVER}/santander/bi-corp/bdr/jm_expedien_bdi';