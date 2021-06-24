CREATE EXTERNAL TABLE IF NOT EXISTS  bi_corp_common.bt_deli_kpi_estadototal (
cod_deli_crupier int,
cod_deli_operacion int,
cod_deli_codigo_barra string,
dt_deli_registro string,
fecha_actualizacion string,
ds_deli_estado string,
ds_deli_operacion string,
ds_deli_paquete_crupier string,
ts_deli_evento string
)
PARTITIONED BY (
	  partition_date string)
STORED AS PARQUET
LOCATION
  '${DATA_LAKE_SERVER}/santander/bi-corp/common/delivery/fact/bt_deli_kpi_estadototal'