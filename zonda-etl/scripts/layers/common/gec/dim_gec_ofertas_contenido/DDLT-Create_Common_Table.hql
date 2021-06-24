CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_common.dim_gec_ofertas_contenido (
				 cod_gec_oferta string
				,cod_gec_canal string
				,ds_gec_canal string
				,ds_gec_ult_accion string
				,cod_gec_periodo_descanso string
				,cod_gec_periodo_presentaciones1 bigint
				,cod_gec_limite_presentaciones1 bigint
				,cod_gec_periodo_presentaciones2 bigint
				,cod_gec_limite_presentaciones2 bigint
				,cod_gec_periodo_presentaciones3 bigint
				,cod_gec_limite_presentaciones3 bigint
				,cod_gec_parametro_1 string
				,cod_gec_parametro_1num decimal(22,4)
				,cod_gec_parametro_2 string
				,cod_gec_parametro_2num decimal(22,4)
				,cod_gec_parametro_3 string
				,cod_gec_parametro_3num decimal(22,4)
				,cod_gec_parametro_4 string
				,cod_gec_parametro_4num decimal(22,4)
				,cod_gec_parametro_5 string
				,cod_gec_parametro_5num decimal(22,4)
				,cod_gec_parametro_6 string
				,cod_gec_parametro_6num decimal(22,4)
				,cod_gec_parametro_7 string
				,cod_gec_parametro_7num decimal(22,4)
				,cod_gec_parametro_8 string
				,cod_gec_parametro_8num decimal(22,4)
				,cod_gec_parametro_9 string
				,cod_gec_parametro_9num decimal(22,4)
				,cod_gec_parametro_10 string
				,cod_gec_parametro_10num decimal(22,4)
				,cod_gec_parametro_11 string
				,cod_gec_parametro_11num decimal(22,4)
				,ts_gec_carga timestamp
				,flag_gec_carrusel int
				,ds_gec_seccion string
				,flag_gec_logoff int
				,cod_gec_hora_inicio int
				,cod_gec_hora_finalizacion int
				,cod_gec_min_inicio int
				,cod_gec_min_finalizacion int
				,cod_gec_audiencias string
                ,partition_date string
)
STORED AS PARQUET
LOCATION
  '${DATA_LAKE_SERVER}/santander/bi-corp/common/gec/dim/dim_gec_ofertas_contenido'
;