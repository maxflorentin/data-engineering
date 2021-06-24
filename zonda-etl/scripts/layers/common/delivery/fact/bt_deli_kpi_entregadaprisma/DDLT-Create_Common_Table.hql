CREATE EXTERNAL TABLE IF NOT EXISTS  bi_corp_common.bt_deli_kpi_entregadaprisma (
cod_deli_crupier int,
cod_deli_operacion int,
cod_deli_producto_tarjeta int,
cod_subproducto int,
completed string,
accepted string,
delta int,
delta_sla int,
delta_hypothetic int,
cod_deli_courier_tracking int,
cod_deli_codigo int,
ds_deli_tipo string,
cod_deli_contract int,
sla int,
sla_hypothetic int,
dt_deli_registro string )
PARTITIONED BY (
	  partition_date string )
STORED AS PARQUET
LOCATION
  '${DATA_LAKE_SERVER}/santander/bi-corp/common/delivery/fact/bt_kpi_entregadaprisma'