CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.rio56_rta_resol_predef (
    cod_entidad  string,
    cod_rta_resol_predef  string,
    desc_rta_resol_predef  string,
    desc_detall_rta_resol  string,
    indi_tpo_rta  string,
    est_rta_resol_predef  string,
    user_alt_rta_resol_predef  string,
    fec_alt_rta_resol_predef  string,
    user_modf_rta_resol_predef  string,
    fec_modf_rta_resol_predef  string,
    cod_tipo_resolucion  string,
    indi_opcion_acm  string,
    sector_owner  string
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
  '${DATA_LAKE_SERVER}/santander/bi-corp/staging/rio56/dim/rta_resol_predef'