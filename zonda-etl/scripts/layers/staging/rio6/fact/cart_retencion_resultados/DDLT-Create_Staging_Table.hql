CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.rio6_cart_retencion_resultados(
    cres_nu_interaccion    string,
    cres_cd_usuario        string,
    cres_fe_proceso        string,
    cres_cd_nacionalidad   string,
    cres_nu_cedula_rif     string,
    cres_nm_cliente        string,
    cres_fe_nacimiento     string,
    cres_tp_resultado      string,
    cres_de_observaciones  string,
    cres_cd_resultado      string
)

PARTITIONED BY (partition_date string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/staging/rio6/fact/cart_retencion_resultados';