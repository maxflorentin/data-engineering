
CREATE EXTERNAL TABLE IF NOT EXISTS  bi_corp_common.bt_deli_kpi_destruidos (
cod_deli_crupier int,
	cod_deli_producto int,
	cod_deli_operacion int,
	cod_deli_paquete int ,
	cod_deli_codigo_barra string,
	custody string,
	destroyed string,
	typecard string,
	delta int,
	delta_sla int,
	delta_hypothetic int,
    sla int,
 sla_hypothetic int,
 ds_deli_operacion string,
 ds_deli_paquete_crupier string,
 dt_deli_registro string
 )

 PARTITIONED BY (
	  partition_date string)
STORED AS PARQUET
LOCATION
  '${DATA_LAKE_SERVER}/santander/bi-corp/common/delivery/fact/bt_deli_kpi_destruidos'