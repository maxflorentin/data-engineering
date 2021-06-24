CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.rio6_cart_canales(
    canl_cd_canal              string,
    canl_de_canal              string,
    canl_nm_apoderado          string,
    canl_nm_cargo_apoderado    string,
    canl_cd_apoderado          string,
    canl_mail_apoderado        string,
    canl_nm_responsable        string,
    canl_in_cons_prestamo      string,
    canl_enum_ramos_cons       string,
    canl_enum_acceso_cons      string
)

PARTITIONED BY (partition_date string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/staging/rio6/dim/cart_canales';