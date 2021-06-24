CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_common.bt_deli_courier_shipping (
  cod_deli_shipping int,
  cod_deli_crupier int,
  cod_deli_courier_tracking STRING,
  ds_deli_courier STRING,
  cod_deli_contract int,
  ds_deli_nombre_cliente STRING,
  cod_deli_documento_cliente int,
  ds_deli_calle STRING,
  cod_deli_altura int,
  cod_deli_piso int,
  ds_deli_departamento STRING,
  ds_deli_provincia STRING,
  cod_deli_postal STRING,
  ds_deli_ciudad STRING,
  cod_suc_sucursal int,
  ts_deli_creacion timestamp,
  flag_deli_cancelado int
)
PARTITIONED BY (
	  partition_date string)
STORED AS PARQUET
LOCATION
  '${DATA_LAKE_SERVER}/santander/bi-corp/common/delivery/fact/bt_deli_courier_shipping';