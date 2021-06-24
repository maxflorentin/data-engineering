create external table if not exists bi_corp_bdr.jm_camb_sit_bdi(
e0688_s1emp  string,
e0688_contra1 string,
e0688_fecini string,
e0688_fecsit string,
e0688_fsalida string,
e0688_fecmarc string,
e0688_idnumcli string,
e0688_bdsitu string,
e0688_ctb_situ string,
e0688_gest_sit string,
e0688_ges2_sit string,
e0688_resp_sit string,
e0688_implicon string,
e0688_impdipto string,
e0688_impvdo string,
e0688_impnovdo string,
e0688_intedev string,
e0688_intemor string,
e0688_comfina string,
e0688_comfinb string,
e0688_gtosdire string,
e0688_gtosindi string,
e0688_indarras string,
e0688_fecultmo string
)
partitioned by (`partition_date` string)
row format serde 'org.apache.hadoop.hive.ql.io.parquet.serde.parquethiveserde'
stored as inputformat 'org.apache.hadoop.hive.ql.io.parquet.mapredparquetinputformat'
outputformat 'org.apache.hadoop.hive.ql.io.parquet.mapredparquetoutputformat'
location '${DATA_LAKE_SERVER}/santander/bi-corp/bdr/jm_camb_sit_bdi';