CREATE EXTERNAL TABLE IF NOT EXISTS BI_CORP_COMMON.BT_OBCM_TRANSACCIONES (
cod_obcm_empresa                    STRING,
cod_obcm_usuario                    STRING,
cod_obcm_transaccion                STRING,
ds_obcm_nom_operacion               STRING,
ds_obcm_nom_producto                STRING,
ds_obcm_nom_reqfrontend             STRING,
ds_obcm_estado_trn                  STRING,
ts_obcm_inicio_trn                  STRING,
ts_obcm_fin_trn                     STRING,
fc_obcm_monto                       DECIMAL(15,4),
cod_per_nup_empresa                 BIGINT,
cod_per_nup_individuo               BIGINT,
cod_obcm_solicitud                  STRING,
cod_obcm_operacion                  STRING,
cod_obcm_estado_sol                 STRING,
fc_obcm_monto_imp1                  DECIMAL(15,4),
cod_div_divisa1                    STRING,
ds_obcm_cant_item1                  INT,
fc_obcm_monto_imp2                  DECIMAL(15,4),
cod_div_divisa2                    STRING,
ds_obcm_cant_item2                  INT

)
PARTITIONED BY (PARTITION_DATE STRING)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/common/interacciones_canales/obcm/fact/bt_obcm_transacciones'