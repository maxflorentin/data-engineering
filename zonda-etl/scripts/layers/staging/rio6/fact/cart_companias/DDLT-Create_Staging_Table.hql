CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.rio6_cart_companias(
    cacm_cd_compania            string,
    cacm_cd_rif                 string,
    cacm_nu_rif                 string,
    cacm_nu_cuit                string,
    cacm_de_compania            string,
    cacm_cd_origen_compania     string,
    cacm_tp_compania            string,
    cacm_di_compania1           string,
    cacm_di_compania2           string,
    cacm_caes_cd_estado         string,
    cacm_caci_cd_ciudad         string,
    cacm_nu_telefono            string,
    cacm_nu_fax                 string,
    cacm_zn_postal              string,
    cacm_nu_apartado            string,
    cacm_fe_exclusion           string,
    cacm_fe_inclusion           string,
    cacm_cjiv_cd_categoria      string,
    cacm_cjgn_cd_condicion      string,
    cacm_in_broker              string,
    cacm_tp_cta_comisiones_db   string,
    cacm_nu_cta_comisiones_db   string
)

PARTITIONED BY (partition_date string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/staging/rio6/fact/cart_companias';