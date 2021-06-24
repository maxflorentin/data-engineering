CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.rio6_asit_clientes(
    ascl_tp_documento     string,
    ascl_nu_documento     string,
    ascl_apellido         string,
    ascl_nombre           string,
    ascl_nu_cuit_cuil     string,
    ascl_nu_telefono      string,
    ascl_sexo             string,
    ascl_fe_nacimiento    string,
    ascl_cd_provincia     string,
    ascl_cd_postal        string,
    ascl_cd_localidad     string,
    ascl_calle            string,
    ascl_numero           string,
    ascl_piso             string,
    ascl_depto            string,
    ascl_mail             string,
    ascl_nu_nup           string
)

PARTITIONED BY (partition_date string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/staging/rio6/dim/asit_clientes';