CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_common.bt_deli_courier_component (
  cod_deli_shipping int,
  cod_deli_codigo bigint,
  ds_deli_tipo STRING,
  cod_deli_componente_tracking STRING,
  ts_deli_evento timestamp
)
PARTITIONED BY (
	  partition_date string)
STORED AS PARQUET
LOCATION
  '${DATA_LAKE_SERVER}/santander/bi-corp/common/delivery/fact/bt_deli_courier_component';
