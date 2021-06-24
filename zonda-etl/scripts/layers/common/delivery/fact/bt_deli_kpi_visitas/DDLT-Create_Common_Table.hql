CREATE EXTERNAL TABLE IF NOT EXISTS  bi_corp_common.bt_deli_kpi_visitas (
delta_first_visit int,
	sla_first_visit int,
	delta_second_visit int,
	sla_second_visit int,
	delta_third_visit int,
	sla_third_visit int,
	hypothetic_delta_first_visit int,
	hypothetic_delta_second_visit int,
	hypothetic_delta_third_visit int,
	delivered string,
	cod_deli_crupier int,
	ds_deli_operacion string,
    ds_deli_paquete_crupier string,
	 dt_deli_registro string )
PARTITIONED BY (
	  partition_date string)
STORED AS PARQUET
LOCATION
  '${DATA_LAKE_SERVER}/santander/bi-corp/common/delivery/fact/bt_deli_kpi_visitas'