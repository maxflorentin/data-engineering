CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.rio56_gc_individuos (
    cod_entidad  string,
    tpo_doc_indi  string,
    nro_doc_indi  string,
    fec_naci_indi  string,
    mar_sex_indi  string,
    ape_indi  string,
    nom_indi  string,
    ide_nup_indi  string,
    cod_suc_indi  string,
    cod_segm_indi  string,
    cod_crm_indi  string,
    semf_ingreso_crm  string,
    semf_renta_crm  string,
    semf_riesgo_crm  string,
    rentabilidad_promedio  string,
    color_semaforo  string,
    color_semaf_riesgo  string
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
  '${DATA_LAKE_SERVER}/santander/bi-corp/staging/rio56/fact/gc_individuos'