CREATE EXTERNAL TABLE IF NOT EXISTS BI_CORP_COMMON.STK_EML_CAMPANIAS (
cod_eml_campania                        string,
ds_eml_campania                         string,
ds_eml_carpeta                          string,
ds_eml_vista                            string,
ds_eml_extid                            string,
ds_eml_proposito                        string,
ds_eml_nombre_lista                     string,
ds_eml_ruta_html                        string,
flag_eml_rastreo_linkcampania           int,
flag_eml_rastreo_analisisterceros       int,
ds_eml_asunto                           string,
ds_eml_nombre_campania                  string,
flag_eml_utf8                           int,
ds_eml_config_regional                  string,
flag_eml_abrio_html                     int,
flag_eml_conversacion                   int,
flag_eml_html_desconocido               int,
ds_eml_baja_campania                    string,
ds_eml_cierre_automitico                string,
ds_eml_mkt_estrategia	                string,
ds_eml_mkt_programa                     string,
ds_eml_email_campania                   string,
ds_eml_email_responda					string
)
PARTITIONED BY (PARTITION_DATE STRING)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/common/interacciones_canales/eml/fact/stk_eml_campanias'