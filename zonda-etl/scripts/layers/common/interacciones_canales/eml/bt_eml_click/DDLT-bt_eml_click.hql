CREATE EXTERNAL TABLE IF NOT EXISTS BI_CORP_COMMON.BT_EML_CLICK (

cod_eml_evento                  string,
cod_eml_cuenta                  string,
cod_eml_lista                   string,
cod_eml_riid                    string,
cod_eml_cliente                 string,
ts_eml_fecha                    string,
ts_eml_fecha_guardado           string,
cod_eml_campania                string,
cod_eml_lanzamiento             string,
ds_eml_email_format             string,
ds_eml_oferta                   string,
ds_eml_numero_oferta            string,
ds_eml_categoria_oferta         string,
ds_eml_oferta_url               string

)
PARTITIONED BY (PARTITION_DATE STRING)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/common/interacciones_canales/eml/fact/bt_eml_click'