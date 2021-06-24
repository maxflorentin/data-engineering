CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.rio6_cart_tipos_cuentas(
    ctcu_cd_cuenta         string,
    ctcu_st_cuenta         string,
    ctcu_in_cuenta         string,
    ctcu_nm_cuenta         string,
    ctcu_nu_desde          string,
    ctcu_nu_hasta          string,
    ctcu_mascara_tarj      string,
    ctcu_pos_num_tarj      string,
    ctcu_long_num_tarj     string,
    ctcu_pos_digito        string,
    ctcu_long_digito       string,
    ctcu_pos_subdig        string,
    ctcu_long_subdig       string,
    ctcu_digito_desde      string,
    ctcu_digito_hasta      string,
    ctcu_subdig_desde      string,
    ctcu_subdig_hasta      string,
    ctcu_fe_ultvcto        string,
    ctcu_fe_equip          string,
    ctcu_cd_sumatoria      string,
    ctcu_cd_calc_dig       string,
    ctcu_div_modulo        string,
    ctcu_regla_validacion  string,
    ctcu_regla_sumatoria   string,
    ctcu_regla_digito      string,
    ctcu_tp_cuenta         string
)

PARTITIONED BY (partition_date string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/staging/rio6/dim/cart_tipos_cuentas';