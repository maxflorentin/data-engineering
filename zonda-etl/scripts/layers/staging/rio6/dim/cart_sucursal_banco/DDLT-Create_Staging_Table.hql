CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.rio6_cart_sucursal_banco(
    capj_cd_sucursal          string,
    capj_de_sucursal          string,
    capj_casu_cd_sucursal     string,
    capj_nu_prox_cotizacion   string,
    capj_nu_prox_declsalud    string,
    capj_cd_centro_emisor     string,
    capj_casu_di_sucursal     string,
    capj_caes_cd_estado       string,
    capj_caci_cd_ciudad       string,
    capj_nu_telefono1         string,
    capj_nu_telefono2         string,
    capj_nu_fax               string,
    capj_zn_postal            string,
    capj_nm_apoderado         string,
    capj_nm_cargo_apoderado   string,
    capj_mail_apoderado       string,
    capj_cd_apoderado         string,
    capj_canl_cd_canal        string,
    capj_in_incentivo         string,
    capj_cd_gestor            string,
    capj_st_sucursal          string,
    capj_fe_status            string
)

PARTITIONED BY (partition_date string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/staging/rio6/dim/cart_sucursal_banco';