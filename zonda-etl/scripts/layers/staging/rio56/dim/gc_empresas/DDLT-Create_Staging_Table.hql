CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.rio56_gc_empresas (
    cod_entidad  string,
    tpo_doc_empr  string,
    nro_doc_empr  string,
    sec_doc_empr  string,
    tpo_empr  string,
    fec_ini_activ_empr  string,
    cod_activ_empr  string,
    nom_comer_empr  string,
    razon_social_empr  string,
    ide_nup_empr  string,
    ape_resp_empr  string,
    nom_resp_empr  string,
    tpo_doc2_empr  string,
    nro_doc2_empr  string,
    cod_suc_empr  string,
    cod_segm_empr  string,
    cod_crm_empr  string,
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
  '${DATA_LAKE_SERVER}/santander/bi-corp/staging/rio56/dim/gc_empresas'