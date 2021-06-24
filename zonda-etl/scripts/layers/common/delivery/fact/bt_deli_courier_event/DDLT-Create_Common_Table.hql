CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_common.bt_deli_courier_event (
  cod_deli_courier_event bigint,
  cod_deli_shipping int,
  ts_deli_evento timestamp,
  cod_suc_sucursal int,
  cod_suc_sucursal_courier int,
  cod_deli_uuid int,
  ts_deli_proceso timestamp,
  cod_deli_estado int,
  ds_deli_estado string,
  ds_deli_motivo string,
  ds_deli_motivo_secundario string,
  cod_deli_estado_courier int
)
PARTITIONED BY (
	  partition_date string)
STORED AS PARQUET
LOCATION
  '${DATA_LAKE_SERVER}/santander/bi-corp/common/delivery/fact/bt_deli_courier_event';

