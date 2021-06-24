CREATE EXTERNAL TABLE IF NOT EXISTS  bi_corp_common.bt_deli_kpi_entregadacorreo (
cod_deli_crupier int,
cod_deli_operacion int,
dt_deli_registro string,
cod_subproducto int,
completed string,
entered string,
delta bigint,
delta_sla int,
delta_hypothetic bigint,
cod_deli_courier_tracking string,
cod_deli_codigo bigint,
ds_deli_tipo string,
cod_deli_contract int,
ds_deli_operacion string,
ds_deli_paquete_crupier string,
sla int,
sla_hypothetic int )

PARTITIONED BY (
	  partition_date string)
STORED AS PARQUET
LOCATION
  '${DATA_LAKE_SERVER}/santander/bi-corp/common/delivery/fact/bt_deli_kpi_entregadacorreo'