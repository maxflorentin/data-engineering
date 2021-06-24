CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.rio56_informacion_adjunta (
    cod_entidad  string,
    ide_gestion_sector  string,
    ide_gestion_nro  string,
    cod_actor  string,
    nro_orden  string,
    indi_tpo_info  string,
    cod_info_doc_reque  string,
    dato_info_doc_reque  string,
    link_inf_adjunta  string,
    fec_inf_adjunta  string,
    user_inf_adjunta  string,
    cod_sect_inf_adjunta  string,
    nom_archivo_original  string,
    nom_archivo  string,
    nom_directorio  string,
    tamano  string,
    secuencia  string
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
  '${DATA_LAKE_SERVER}/santander/bi-corp/staging/rio56/dim/informacion_adjunta'