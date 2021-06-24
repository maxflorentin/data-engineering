CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_common.bt_ate_cuentas_digitales (
        cod_ate_solicitud int
        ,cod_per_nup int
        ,cod_suc_sucursal int
        ,cod_ate_producto int
        ,ds_ate_producto string
        ,ds_ate_estado_solicitud string
        ,ds_ate_dispositivo string
        ,ts_ate_creacion timestamp
        ,ts_ate_alta timestamp
        ,cod_ate_asol string
        ,ds_ate_numdoc BIGINT
        ,cod_ate_estado_asol string
        ,cod_cue_cuenta bigint
        ,cod_ate_canal string
        ,cod_ate_biometria string
        ,cod_ate_app string
        ,cod_suc_sucursal_cr int
        ,cod_ate_cajero string
        ,ds_ate_nombre string
        ,ds_ate_apellido string
        ,cod_ate_tramite_renaper string
        ,cod_ate_nacionalidad string
        ,cod_ate_pais_origen string
        ,dt_ate_fecha_nacimiento string
        ,cod_ate_sexo string
        ,cod_ate_estado_civil string
        ,ds_ate_email string
        ,ds_ate_celular string
        ,ds_ate_compania_celular string
        ,ds_ate_cuil BIGINT
        ,cod_ate_tipo_laboral string
        ,fc_ate_ingresos bigint
        ,flag_ate_universal int
        ,flag_ate_acepto_tyc int
        ,flag_ate_genero_clave int
        ,ts_ate_inicio_actividad timestamp
        ,cod_ate_tipo_residencia string
        ,ts_ate_expiracion_doc timestamp
        ,cod_ate_ejemplar string
        ,cod_ate_actividad string
        ,cod_ate_similitud int
        ,flag_ate_sanidad int
        ,ts_ate_emision timestamp
        ,ts_ate_vto_residencia timestamp
        ,flag_ate_cliente_con_cuenta int
        ,ds_ate_localidad string
        ,ds_ate_provincia string
        ,ds_ate_cod_postal string
        ,ds_ate_origen string
        ,ds_ate_medium string
        ,ds_ate_campaign string
        ,ds_ate_cod_area string
        ,ds_ate_nro_telefono string
)
PARTITIONED BY (
      partition_date string)
STORED AS PARQUET
LOCATION
  '${DATA_LAKE_SERVER}/santander/bi-corp/common/atenea/fact/bt_ate_cuentas_digitales'