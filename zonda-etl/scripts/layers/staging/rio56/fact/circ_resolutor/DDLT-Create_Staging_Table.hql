CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.rio56_circ_resolutor (
    cod_entidad string,
    cod_prior_circ string,
    cod_sector string,
    est_circ_resol string,
    fec_alt_circ_resol string,
    fec_modf_circ_resol string,
    ide_circuito string,
    ind_obl_resp string,
    nro_ord_circ string,
    tmp_circ_resol string,
    user_alt_circ_resol string,
    user_modf_circ_resol string
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
  '${DATA_LAKE_SERVER}/santander/bi-corp/staging/rio56/fact/circ_resolutor'