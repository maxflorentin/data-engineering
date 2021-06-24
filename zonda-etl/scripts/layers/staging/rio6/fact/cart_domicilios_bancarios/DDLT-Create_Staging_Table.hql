CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.rio6_cart_domicilios_bancarios(
    cadm_cacn_cd_nacionalidad   string,
    cadm_cacn_nu_cedula_rif     string,
    cadm_nu_domicilio           string,
    cadm_caba_cd_banco          string,
    cadm_nu_cuenta              string,
    cadm_tp_cuenta              string,
    cadm_in_empleado            string,
    cadm_tp_cuenta_banco        string,
    cadm_st_cuenta              string,
    cadm_cd_causa_estado        string,
    cadm_fe_act_estado          string,
    cadm_cd_usuario_estado      string
)

PARTITIONED BY (partition_date string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/staging/rio6/fact/cart_domicilios_bancarios';