CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_common.bt_trf_transferencias (
    cod_trf_entidad_origen          string,
    cod_trf_tipo_entidad_origen     string,
    ds_trf_entidad_origen           string,
    cod_trf_suc_origen              bigint,
    cod_trf_cta_origen              bigint,
    ds_per_cuit_origen              string,
    cod_trf_cbu_origen              string,
    ds_per_nombre_origen            string,
    cod_per_nup_origen              bigint,
    cod_trf_entidad_destino         string,
    cod_trf_tipo_entidad_destino    string,
    ds_trf_entidad_destino          string,
    cod_trf_suc_destino             bigint,
    cod_trf_cta_destino             bigint,
    ds_per_cuit_destino	            string,
    cod_trf_cbu_destino             string,
    ds_per_nombre_destino           string,
    cod_per_nup_destino             bigint,
    ts_trf_fecha_estado             string,
    ts_trf_fecha_alta               string,
    ts_trf_fecha_debito             string,
    ts_trf_fecha_env_riesgo         string,
    ts_trf_fecha_credito            string,
    ts_trf_fecha_env_camara         string,
    ts_trf_fecha_rta_camara         string,
    cod_trf_concepto                string,
    ds_trf_concepto                 string,
    ds_trf_referencia               string,
    fc_trf_importe                  decimal(15,2),
    cod_trf_moneda                  string,
    cod_trf_producto                string,
    ds_trf_producto                 string,
    cod_trf_camara                  string,
    ds_trf_camara                   string,
    cod_trf_canal                   string,
    ds_trf_canal                    string,
    ds_trf_tipo                     string,
    cod_trf_en_camara               string,
    cod_trf_estado                  string,
    cod_trf_mot_rechazo             string,
    ds_trf_mot_rechazo              string,
    cod_trf_origen                  string
)
PARTITIONED BY (
    partition_date          string)

ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  '${DATA_LAKE_SERVER}/santander/bi-corp/common/transferencias/fact/bt_trf_transferencias';
