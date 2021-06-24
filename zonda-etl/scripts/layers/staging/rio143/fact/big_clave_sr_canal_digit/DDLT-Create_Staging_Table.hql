CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.rio143_claves_sr_canales(
       entidad                     string,
       nup                         string,
       sesion                      string,
       fase                        string,
       sysid                       string,
       task                        string,
       trmid                       string,
       userid                      string,
       trnid                       string,
       servicio                    string,
       canal                       string,
       subcanal                    string,
       ret_code                    string,
       ret_texto                   string,
       stamp                       string,
       id_rc                       string,
       id_ciclo                    string,
       id_sw_sistema               string,
       id_sw_preguntas             string,
       id_sw_autenticacion         string,
       id_sw_pin                   string,
       id_sw_cvv2                  string,
       id_sw_otp                   string,
       id_tipo                     string,
       id_numero                   string,
       nro_doc                     string,
       fec_nac                     string,
       pd_rc                       string,
       pd_ciclo                    string,
       pd_id                       string,
       pd_stamp                    string,
       au_rc                       string,
       au_ciclo                    string,
       au_tipo                     string,
       au_numero                   string,
       au_offset                   string,
       ck_rc                       string,
       ck_ciclo                    string,
       detalle						string

)

PARTITIONED BY (partition_date string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/staging/rio143/fact/claves_sr_canales';