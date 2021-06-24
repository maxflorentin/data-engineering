CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.rio6_cart_operaciones_diarias(
    caod_caop_cd_operacion         string,
    caod_cd_sub_operacion          string,
    caod_fe_operacion              string,
    caod_hr_operacion              string,
    caod_cd_usuario                string,
    caod_st_a                      string,
    caod_st_b                      string,
    caod_st_c                      string,
    caod_de_terminal               string,
    caod_casu_cd_sucursal          string,
    caod_carp_cd_ramo              string,
    caod_capo_nu_poliza            string,
    caod_cace_nu_certificado       string,
    caod_cacn_cd_nacionalidad      string,
    caod_cacn_nu_cedula_rif        string,
    caod_caps_cd_proveedor         string,
    caod_care_nu_recibo            string,
    caod_cain_nu_inspeccion        string,
    caod_caso_nu_solicitud         string,
    caod_capd_cd_productor         string,
    caod_sisi_nu_siniestro         string,
    caod_sims_nu_movimiento        string,
    caod_cd_accion_evento          string,
    caod_cazc_cd_zona_cobro        string,
    caod_calo_cd_localidad         string,
    caod_casu_cd_suc_impresion     string,
    caod_caur_unidad_produccion    string,
    caod_cadm_nu_domicilio         string,
    caod_capj_cd_sucursal_banco    string,
    caod_siglas_interface          string,
    caod_st_interface              string,
    caod_capu_cd_producto          string,
    caod_nu_endoso                 string,
    caod_fe_interface              string,
    caod_de_operacion              string,
    caod_de_endoso                 string,
    caod_de_define_endoso          string,
    caod_fe_endoso                 string,
    caod_cd_suc_debito             string
)

PARTITIONED BY (partition_date string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/staging/rio6/fact/cart_operaciones_diarias';