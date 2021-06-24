CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.rio56_circ_info_reque (
    cod_entidad  string,
    ide_circuito  string,
    indi_info_gpo  string,
    cod_info_reque  string,
    indi_info_oblig  string,
    est_circ_info_reque  string,
    user_alt_circ_info_reque  string,
    fec_alt_circ_info_reque  string,
    user_modf_circ_info_reque  string,
    fec_modf_circ_info_reque  string,
    orden_info  string,
    cod_etapa  string,
    orden_etapa  string
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
  '${DATA_LAKE_SERVER}/santander/bi-corp/staging/rio56/dim/circ_info_reque'