CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.rio6_cart_cotiza_personas(
    cacj_capj_cd_sucursal    string,
    cacj_cazb_nu_cotizacion  string,
    cacj_nu_consecutivo      string,
    cacj_cd_rol              string,
    cacj_nm_persona          string,
    cacj_cd_sexo             string,
    cacj_fe_nac              string,
    cacj_tp_persona          string,
    cacj_in_fumador          string,
    cacj_po_partic           string,
    cacj_cd_parentesco       string,
    cacj_tp_documento        string,
    cacj_nu_documento        string,
    cacj_mt_sumaseg_renta    string,
    cacj_nu_prioridad        string,
    cacj_tp_cuenta           string,
    cacj_nu_cuenta           string
)

PARTITIONED BY (partition_date string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/staging/rio6/fact/cart_cotiza_personas';