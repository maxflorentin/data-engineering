
CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_common.bt_cc_genesysdiscador (

id_cc_registro                  string,
ts_cc_fecha                     string,
cod_cc_accion                   string,
ds_cc_accion                    string,
cod_cc_genesys                  string,
cod_cc_lista                    string,
ds_cc_lista                     string,
cod_cc_campania                 string,
ds_cc_campania                  string,
cod_cc_grupo                    string,
ds_cc_grupo                     string,
cod_cc_ocs_app                  string,
cod_cc_inquilino                string,
ds_cc_inquilino                 string,
cod_cc_conexion                 string,
cod_cc_tipo_telefono            string,
ds_cc_telefono                  string,
cod_cc_tipo_registro            string,
cod_cc_estado_registro          string,
cod_cc_resultado_llamada        string,
ds_cc_resultado_llamada         string,
ds_cc_num_intentos              string,
ts_cc_llamada_programada        string,
ts_cc_llamada                   string,
ds_cc_diariamente_desde         string,
ds_cc_diariamente_hasta         string,
ds_cc_agente                    string,
cod_cc_cadena                   string,
ds_cc_datosm                    string,
ds_cc_nombre_tabla				string
)
PARTITIONED BY (
    partition_date string)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
'${DATA_LAKE_SERVER}/santander/bi-corp/common/interacciones_canales/cc/fact/bt_cc_genesysdiscador'
