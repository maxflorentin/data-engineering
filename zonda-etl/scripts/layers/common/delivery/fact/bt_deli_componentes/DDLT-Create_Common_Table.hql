CREATE EXTERNAL TABLE IF NOT EXISTS  bi_corp_common.bt_deli_componentes (
				 cod_deli_componente int
				,cod_deli_crupier int
				,cod_per_nup int
				,cod_deli_producto_componente int
				,ds_deli_producto_componente string
				,cod_deli_nro_tarjeta_componente string
				,cod_deli_nro_novedad_amas int
				,cod_deli_nro_secuencia_amas int
				,cod_deli_producto_tarjeta int
				,cod_deli_subproducto_tarjeta int
				,ds_deli_producto_tarjeta string
				,cod_deli_origen_producto_tarjeta int
				,cod_deli_color_producto_tarjeta int
				,cod_deli_marca_adicional int
				,dt_deli_ultima_modificacion_componente string
				,cod_deli_cuentamcnro string
				,ds_deli_creador_componente string
				,ds_deli_ultimo_modificador_componente string
				,cod_deli_marca_retenido int
				,cod_deli_nro_tarjeta_rechazo_abae string
				,ds_deli_color_producto_tarjeta string
				,cod_deli_secuencia bigint
)
PARTITIONED BY (
	  partition_date string)
STORED AS PARQUET
LOCATION
  '${DATA_LAKE_SERVER}/santander/bi-corp/common/delivery/fact/bt_deli_componentes'