CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_common.bt_gec_ofertas_presentadas (
                cod_gec_oferta string
				,cod_gec_oferta_interno string
				,cod_gec_canal string
				,ds_gec_canal string
				,cod_per_nup int
				,ds_gec_accion string
				,ts_gec_presentacion timestamp
				,ds_gec_tipo_oferta string
				,ds_gec_categoria_oferta string
				,cod_gec_grupo_control string
				,ts_gec_carga timestamp
				,cod_gec_probabilidad decimal(22,6)
				,flag_gec_carrusel int
				,ds_gec_ubicacion_seccion string
				,cod_gec_rtd string
				,ts_gec_ult_actualizacion timestamp
				,cod_gec_orden_prioridad string
				,cod_gec_tipo_ofrecimiento string
				,ds_gec_origen int
)
PARTITIONED BY (
      partition_date string)
STORED AS PARQUET
LOCATION
  '${DATA_LAKE_SERVER}/santander/bi-corp/common/gec/fact/bt_gec_ofertas_presentadas'