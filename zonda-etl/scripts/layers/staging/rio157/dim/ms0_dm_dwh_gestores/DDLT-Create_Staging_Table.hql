CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.rio157_ms0_dm_dwh_gestores (
    cod_pais  string,
    cod_gestor  string,
    des_gestor  string,
    cod_entidad  string,
    cod_oficial  string,
    cod_ofi_gestora  string,
    nom_cargo_gestor  string,
    cod_tip_oficial  string,
    num_doc_id  string,
    fec_alta  string,
    fec_baja  string,
    cod_proceso  string,
    ind_activo  string,
    userid_umo  string,
    timest_umo  string
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
      '${DATA_LAKE_SERVER}/santander/bi-corp/staging/rio157/dim/ms0_dm_dwh_gestores'