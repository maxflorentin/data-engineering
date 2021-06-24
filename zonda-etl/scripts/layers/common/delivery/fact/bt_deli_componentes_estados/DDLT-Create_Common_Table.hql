CREATE EXTERNAL TABLE IF NOT EXISTS  bi_corp_common.bt_deli_componentes_estados
(
				cod_deli_componente_envio int
				,cod_deli_estado int
				,dt_deli_novedad_estado string
				,dt_deli_ultima_modificacion_estado string
				,ds_deli_creador_estado string
				,ds_deli_ultimo_modificador_estado string
				,cod_deli_error string
				,ds_deli_error string
				,cod_deli_componente int
				,ds_deli_estado_crupier_comp string
)
PARTITIONED BY (
	  partition_date string)
STORED AS PARQUET
LOCATION
  '${DATA_LAKE_SERVER}/santander/bi-corp/common/delivery/fact/bt_deli_componentes_estados'