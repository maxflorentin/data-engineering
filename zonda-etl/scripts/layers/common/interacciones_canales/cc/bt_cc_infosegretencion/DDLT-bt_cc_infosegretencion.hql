
CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_common.bt_cc_infosegretencion (

ts_cc_fecha                     string,
ds_cc_cuestionario              string,
ds_cc_signigicado               string,
cod_cc_sucursal                 string,
cod_cc_legajo                   string,
ds_cc_apellido_nombre           string,
cod_cc_ramonuevo                string,
ds_cc_ramo                      string,
ds_cc_num_polizanuevo           string,
ds_cc_num_certificadonuevo      string,
cod_cc_producto                 string,
cod_cc_sucursal_emision         string,
ds_cc_sucursal                  string,
cod_cc_canal                    string,
ds_cc_canal                     string,
cod_cc_estado_certificado       string,
ds_cc_estado                    string,
cod_cc_nacionalidad             string,
ds_cc_num_cedularif             string,
ds_cc_apellido_razon            string,
dt_cc_fecha_hasta               string,
dt_cc_fecha_comision            string,
cod_cc_ramo                     string,
ds_cc_num_poliza                string,
ds_cc_num_certificado           string,
cod_per_nup                     bigint
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
  '${DATA_LAKE_SERVER}/santander/bi-corp/common/interacciones_canales/cc/fact/bt_cc_infosegretencion'