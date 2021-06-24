CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_common.bt_deli_envios (
  cod_deli_crupier INT,
  cod_per_nup INT,
  cod_suc_sucursal INT,
  cod_deli_solicitud INT,
  cod_deli_nro_paquete INT,
  cod_deli_nro_cta_paquete INT,
  cod_deli_operacion INT,
  ds_deli_operacion STRING,
  dt_deli_registro STRING,
  cod_deli_canal_venta STRING,
  ds_deli_canal_venta STRING,
  cod_deli_ejecutivo_comercial STRING,
  ds_deli_calle STRING,
  cod_deli_altura INT,
  cod_deli_piso STRING,
  ds_deli_depto STRING,
  cod_deli_postal STRING,
  ds_deli_localidad STRING,
  ds_deli_telefono STRING,
  cod_deli_paquete INT,
  ds_deli_paquete_crupier STRING,
  cod_deli_codigo_barra STRING,
  dt_deli_ultima_modificacion STRING,
  ds_deli_creador STRING,
  ds_deli_ultimo_modificador STRING,
  cod_deli_producto INT,
  cod_deli_subproducto STRING,
  ds_deli_tipo_dom STRING,
  cod_deli_exp_partial INT,
  flag_deli_finalizado INT,
  ds_deli_courier STRING
)
PARTITIONED BY (
      partition_date string)
STORED AS PARQUET
LOCATION
  '${DATA_LAKE_SERVER}/santander/bi-corp/common/delivery/fact/bt_deli_envios'