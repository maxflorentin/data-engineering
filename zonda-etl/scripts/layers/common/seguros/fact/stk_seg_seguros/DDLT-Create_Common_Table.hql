CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_common.stk_seg_seguros (
cod_per_nup                           BIGINT,
cod_seg_poliza                        BIGINT,
cod_seg_certificado                   BIGINT,
cod_seg_poliza_original               BIGINT,
cod_seg_certificado_original          BIGINT,
cod_seg_tipo_seguro                   string,
ds_seg_tipo_seguro                    string,
cod_seg_ramo                          BIGINT,
ds_seg_ramo                           string,
cod_seg_producto                      string,
ds_seg_producto                       string,
cod_seg_plan                          string,
ds_seg_plan                           string,
cod_seg_clase                         string,
ds_seg_clase                          string,
cod_seg_zona                          BIGINT,
cod_seg_sucursal                      BIGINT,
ds_seg_sucursal                       string,
cod_seg_canal                         BIGINT,
ds_seg_canal                          string,
ds_seg_agrupador_canales              string,
cod_seg_tipo_mercado                  string,
ds_seg_tipo_mercado                   string,
dt_seg_fecha_alta_poliza_original     string,
dt_seg_fecha_suscripcion              string,
dt_seg_fecha_desde_vigencia           string,
dt_seg_fecha_hasta_vigencia           string,
dt_seg_fecha_proxima_facturacion      string,
ds_seg_tipo_doc                       string,
ds_seg_num_doc                        BIGINT,
ds_per_nombre                         string,
fc_seg_suma_asegurada                 BIGINT,
fc_seg_premio                         decimal(15,2),
fc_seg_prima                          decimal(15,2),
fc_seg_comision                       decimal(15,2),
ds_seg_tipo_doc_tomador               string,
ds_seg_num_doc_tomador                BIGINT,
ds_seg_nombre_beneficiario            string,
cod_seg_forma_pago                    string,
ds_seg_forma_pago                     string,
cod_seg_tipo_cuenta                   string,
ds_seg_tipo_cuenta                    string,
cod_seg_sucursal_cuenta               BIGINT,
cod_seg_cuenta                        BIGINT,
ds_seg_provincia                      string,
ds_seg_cod_postal                     string,
ds_seg_ciudad                         string,
ds_seg_domicilio                      string,
cod_seg_tipo_vehiculo                 string,
ds_seg_tipo_vehiculo                  string,
cod_seg_serial_carroceria             string,
cod_seg_serial_motor                  string,
cod_seg_marca                         string,
ds_seg_marca                          string,
cod_seg_modelo                        string,
ds_seg_modelo                         string,
int_seg_ano_fabricacion               BIGINT,
cod_seg_placa                         string,
cod_seg_tipo_vivienda                 string,
ds_seg_tipo_vivienda                  string,
cod_seg_sup_cubierta                  BIGINT,
ds_seg_compania                       string,
flag_seg_poliza_digital               int
)
PARTITIONED BY (
  partition_date string)
STORED AS PARQUET
LOCATION
  '${DATA_LAKE_SERVER}/santander/bi-corp/common/seguros/fact/stk_seg_seguros'