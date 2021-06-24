
CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_common.stk_tcr_tarjetas
(
    cod_tcr_tipo_cuenta                string,
    cod_tcr_cuenta                     bigint,
    cod_tcr_tarjeta                    bigint,
    cod_tcr_marca                      string,
    cod_per_nup_tarjeta                int,
    cod_per_nup_titular                int,
    ds_tcr_apellido_nombre_embozado    string,
    cod_tcr_sucursal                   int,
    ds_tcr_aplicacion_relacionada      string,
    cod_suc_sucursal       int,
    cod_tcr_tipo_cuenta_relacionada    int,
    cod_nro_cuenta         bigint,
    int_tcr_nro_firmas_relacionada     int,
    cod_tcr_categoria                  int,
    cod_tcr_estado_cuenta              int,
    cod_tcr_estado_tarjeta             int,
    cod_tcr_producto                   string,
    cod_prod_producto            string,
    cod_prod_subproducto         string,
    ds_tcr_producto_tipo               string,
    ds_tcr_producto_agrup              string,
    dt_tcr_fecha_incluye_boletin       string,
    cod_tcr_motivo_incluye_boletin      int,
    dt_tcr_fecha_alta                  string,
    cod_tcr_motivo_alta                 int,
    cod_tcr_usuario_alta               string,
    dt_tcr_ult_modif_fec               string,
    dt_tcr_fecha_baja                  string,
    cod_tcr_motivo_baja                 int,
    cod_tcr_solicitud                  bigint,
    cod_tcr_campania                   string,
    cod_tcr_campania_corto             int,
    flag_tcr_marca_contrato_citi       int,
    flag_tcr_marca_preembozada         int,
    ds_tcr_campania_tipo               string,
    cod_tcr_vigencia_mes_desde         string,
    cod_tcr_vigencia_mes_hasta         string,
    cod_tcr_vigencia_anio_desde        string,
    cod_tcr_vigencia_anio_hasta        string,
    cod_tcr_motivo_no_renovacion       string,
    ds_tcr_motivo_no_renovacion        string,
    dt_tcr_fecha_no_renovacion         string,
    cod_tcr_tipo_tarjeta               string,
    ds_tcr_tipo_tarjeta                string,
    cod_tcr_canal_venta                int,
    ds_tcr_canal_venta                 string,
    cod_tcr_marca_tarjeta              string,
    ds_tcr_autorizado_tc               string,
    flag_tcr_titular                   int,
    cod_tcr_grupo_afinidad             string,
    ds_tcr_grupo_afinidad              string,
    cod_tcr_tipo_promo                 string,
    dt_tcr_fecha_promo                 string,
    fc_tcr_porcentaje_limite_compra    decimal(15,2),
    fc_tcr_porcentaje_limite_cuotas    decimal(15,2),
    fc_tcr_porcentaje_limite_adelantos decimal(15,2)
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
        '${DATA_LAKE_SERVER}/santander/bi-corp/common/tarjetas/fact/stk_tcr_tarjetas'