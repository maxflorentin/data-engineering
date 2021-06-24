CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.rio6_cart_siniestros_age_inst(
    csai_nu_orden                 string,
    csai_fe_registracion          string,
    csai_fe_siniestro             string,
    csai_st_siniestro             string,
    csai_cd_compania              string,
    csai_fe_comunicacion          string,
    csai_carp_cd_ramo             string,
    csai_capo_nu_poliza           string,
    csai_cace_nu_certificado      string,
    csai_cace_fe_desde            string,
    csai_cace_fe_hasta            string,
    csai_nm_aseg                  string,
    csai_nm_beneficiario1         string,
    csai_nm_beneficiario2         string,
    csai_nm_beneficiario3         string,
    csai_nm_beneficiario4         string,
    csai_nm_beneficiario5         string,
    csai_de_siniestro             string,
    csai_nu_siniestro             string,
    csai_tp_documento             string,
    csai_nu_documento             string
)

PARTITIONED BY (partition_date string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/staging/rio6/fact/cart_siniestros_age_inst';