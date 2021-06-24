
CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_common.bt_cc_infomartinteraccion (

cod_cc_interaccion                      string,
dt_cc_fecha                             string,
ds_cc_intervalo                         string,
ds_cc_tipo_interaccion                  string,
ds_cc_canal                             string,
ds_cc_asunto                            string,
ds_cc_last_queue                        string,
ds_cc_last_vqueue                       string,
ds_cc_from                              string,
ds_cc_apellido                          string,
ds_cc_nombre                            string,
cod_per_nup                             bigint,
ds_cc_numdoc                            bigint,
ds_cc_cod_disposicion                   string,
ds_cc_motivo_cierre                     string,
ts_cc_inicio_gestioninteraccion         string,
ts_cc_fin_gestioninteraccion            string,
ds_cc_agente                            string,
cod_cc_legajo                           string,
ts_cc_fin_interaccion                   string,
ts_cc_inicio_interaccion                string

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
  '${DATA_LAKE_SERVER}/santander/bi-corp/common/interacciones_canales/cc/fact/bt_cc_infomartinteraccion'

