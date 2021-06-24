CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.malbgc_zbdtmig(
mig_old_entidad string,
mig_old_cent_alta string,
mig_old_cuenta string,
mig_old_divisa string,
mig_new_entidad string,
mig_new_cent_alta string,
mig_new_cuenta string,
mig_new_divisa string,
mig_old_fech_baja string,
mig_old_hora_baja string,
mig_old_motiv_baja  string,
mig_old_motiv_migr  string,
mig_old_indesta string,
mig_new_indesta string,
mig_entidad_umo string,
mig_centr_umo  string,
mig_userid_umo string,
mig_netname_umo string,
mig_timest_umo string
)
PARTITIONED BY (
  partition_date string)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  '${DATA_LAKE_SERVER}/santander/bi-corp/staging/malbgc/fact/zbdtmig'