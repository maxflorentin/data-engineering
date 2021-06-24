CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.rio6_cart_usuarios(
    caus_cd_usuario              string,
    caus_cagr_cd_compania        string,
    caus_cagr_cd_unidad_adm      string,
    caus_nm_usuario              string,
    caus_cd_nivel                string,
    caus_st_usuario              string,
    caus_fe_ingreso              string,
    caus_cd_impresora_fija       string,
    caus_cd_impresora_opcional   string,
    caus_cd_sucursal             string,
    caus_cd_categoria            string,
    caus_cd_agencia              string,
    caus_cd_impresora_cuad       string,
    caus_capj_cd_sucursal        string,
    caus_cd_password             string,
    caus_cemp_cd_suc_empresa     string,
    caus_cd_usuario_racf         string,
    caus_dir_email               string,
    caus_cd_perfil               string,
    caus_cd_centro_costos        string,
    caus_de_puesto               string,
    caus_wepe_cd_perfil          string,
    caus_nu_fallos               string
)

PARTITIONED BY (partition_date string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/staging/rio6/fact/cart_usuarios';