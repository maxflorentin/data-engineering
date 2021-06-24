CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.rio56_circ_autorizante (
    cod_entidad string,
    cod_perfil string,
    cod_prioridad string,
    cod_sector string,
    est_circ_autz string,
    fec_alt_circ_autz string,
    fec_modf_circ_autz string,
    ide_circuito string,
    imp_max_autz string,
    imp_min_autz string,
    indi_mail_autz string,
    indi_selec_autz string,
    nro_ord_autz string,
    tmp_circ_autz string,
    user_alt_circ_autz string,
    user_modf_circ_autz string
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
  '${DATA_LAKE_SERVER}/santander/bi-corp/staging/rio56/fact/circ_autorizante'