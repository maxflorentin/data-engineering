CREATE EXTERNAL TABLE IF NOT EXISTS BI_CORP_COMMON.BT_OBE_TRANSACCIONES (
ts_obe_fecha                    string,
ds_obe_cuit                     bigint,
cod_obe_transaccion             string,
ds_obe_transaccion              string,
cod_obe_modulo                  string,
ds_obe_modulo                   string,
cod_obe_tipo_transaccion         string,
ds_obe_tipo_transaccion         string,
cod_obe_submodulo               string,
ds_obe_submodulo                string,
cod_obe_error                   string,
ds_obe_pid                      string,
ds_obe_comprobante              string,
fc_obe_importe                  decimal(19,4),
ds_obe_moneda                   string,
cod_obe_sucursal                int,
cod_obe_estado                  string,
cod_obe_solicitud               string,
ds_obe_nro_cuenta               string,
ds_obe_tipo_cuentacrd           string,
cod_obe_sucursal_crd            int,
ts_obe_fecha_alta               string,
ds_obe_tipo_empresa             string,
ds_obe_destinatario             string,
ds_obe_tipo_teclado             string,
cod_per_nup                     bigint,
ds_obe_rpi_mediopago            string,
ds_obe_rpi_cuitempsev           bigint,
ds_obe_rpi_servicio             string,
ds_obe_rpi_nomfantasia          string,
ds_obe_rpi_identclienteemp		string
)
PARTITIONED BY (PARTITION_DATE STRING)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/common/interacciones_canales/obe/fact/bt_obe_transacciones'