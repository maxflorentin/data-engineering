CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.rio56_gc_empresa_gestiones (
    cod_entidad  string,
    ide_gestion_sector  string,
    ide_gestion_nro  string,
    tpo_doc_empr  string,
    nro_doc_empr  string,
    sec_doc_empr  string
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
  '${DATA_LAKE_SERVER}/santander/bi-corp/staging/rio56/dim/gc_empresa_gestiones'