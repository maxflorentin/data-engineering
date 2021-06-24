CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_common.dim_gec_ofertas (
				cod_gec_oferta string
				,cod_gec_oferta_interno string
				,ds_gec_categoria_oferta string
				,ds_gec_tipo_oferta string
				,ds_gec_segmento_aplica string
				,ds_gec_oferta string
				,ts_gec_inicio timestamp
				,ts_gec_finalizacion timestamp
				,cod_gec_pg1_score decimal(22,4)
				,cod_gec_pg2_score decimal(22,4)
				,cod_gec_pg3_score decimal(22,4)
				,cod_gec_pg4_score decimal(22,4)
				,ds_gec_localidad string
				,ds_gec_provincia string
				,fc_gec_cant_puntos_base bigint
				,ds_gec_url string
				,ds_gec_dia_aplica string
				,ts_gec_carga timestamp
				,cod_gec_seccion string
				,cod_gec_loyalty string
				,cod_gec_producto string
				,ds_gec_familia string
				,cod_gec_valida_producto_familia int
                ,partition_date string
)
STORED AS PARQUET
LOCATION
  '${DATA_LAKE_SERVER}/santander/bi-corp/common/gec/dim/dim_gec_ofertas'
;