CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_common.stk_adm_empresas_rating_plus (
    dec_adm_re_producto_demanda_mercado decimal(4, 2),
    dec_adm_re_accionistas_gerencia decimal(4, 2),
    dec_adm_re_acceso_credito decimal(4, 2),
    dec_adm_re_beneficios_rentabilidad decimal(4, 2),
    dec_adm_re_generacion_recursos decimal(3, 2),
    dec_adm_re_solvencia decimal(3, 2),
    dec_adm_re_rating_matriz decimal(16, 2),
    int_adm_re_probabilidad_apoyo_matriz int,
    dec_adm_re_valoracion_final decimal(3, 2),
    dec_adm_re_apoyo_casa_matriz decimal(5, 2),
    dec_adm_re_valoracion_estadistica decimal(3, 2),
    ds_adm_rating_experto string,
    dec_adm_ree_rating_estadistico decimal(16, 2),
    dec_adm_ree_rating_apoyo_matriz decimal(16, 2),
    dec_adm_ree_rating_estadistico_experto decimal(16, 2),
    dec_adm_rating_estadistico_experto decimal(16, 2),
    ds_adm_ree_motivo_no_aplica string,
    ds_adm_ree_motivos_desacuerdo string,
    dt_adm_valoracion string,
    id_adm_ree_balance_1 int,
    id_adm_ree_balance_2 int,
    ds_adm_obs_acc_ger string,
    ds_adm_obs_pro_dem_mer string,
    ds_adm_obs_acc_cred string,
    ds_adm_obs_ben_rentab string,
    ds_adm_obs_gen_recursos string,
    ds_adm_obs_solvencia string,
    ds_adm_obs_proyecciones string,
    ds_adm_obs_exp_ant string,
    ds_adm_obs_sintesis string,
    ds_adm_resumen_act string,
    ds_adm_bullet_points string,
    ds_adm_comentarios string,
    ds_adm_riesgos string,
    ds_adm_mitigantes string,
    ds_adm_periodo string, --
    cod_adm_tipo_rating int,
    ds_adm_tipo_rating string,
    cod_adm_area_1 int,
    cod_adm_pregunta_1 int,
    cod_adm_respuesta_1 int,
    ds_adm_area_1 string,
    ds_adm_preguntas_1 string,
    ds_adm_respuestas_1 string,
    cod_adm_area_2 int,
    cod_adm_pregunta_2 int,
    cod_adm_respuesta_2 int,
    ds_adm_area_2 string,
    ds_adm_preguntas_2 string,
    ds_adm_respuestas_2 string,
    cod_adm_area_3 int,
    cod_adm_pregunta_3 int,
    cod_adm_respuesta_3 int,
    ds_adm_area_3 string,
    ds_adm_preguntas_3 string,
    ds_adm_respuestas_3 string,
    cod_adm_area_4 int,
    cod_adm_pregunta_4 int,
    cod_adm_respuesta_4 int,
    ds_adm_area_4 string,
    ds_adm_preguntas_4 string,
    ds_adm_respuestas_4 string,
    cod_adm_area_5 int,
    cod_adm_pregunta_5 int,
    cod_adm_respuesta_5 int,
    ds_adm_area_5 string,
    ds_adm_preguntas_5 string,
    ds_adm_respuestas_5 string,
    cod_adm_area_6 int,
    cod_adm_pregunta_6 int,
    cod_adm_respuesta_6 int,
    ds_adm_area_6 string,
    ds_adm_preguntas_6 string,
    ds_adm_respuestas_6 string,
    cod_adm_area_7 int,
    cod_adm_pregunta_7 int,
    cod_adm_respuesta_7 int,
    ds_adm_area_7 string,
    ds_adm_preguntas_7 string,
    ds_adm_respuestas_7 string,
    cod_adm_area_8 int,
    cod_adm_pregunta_8 int,
    cod_adm_respuesta_8 int,
    ds_adm_area_8 string,
    ds_adm_preguntas_8 string,
    ds_adm_respuestas_8 string,
    cod_adm_area_9 int,
    cod_adm_pregunta_9 int,
    cod_adm_respuesta_9 int,
    ds_adm_area_9 string,
    ds_adm_preguntas_9 string,
    ds_adm_respuestas_9 string,
    cod_adm_area_10 int,
    cod_adm_pregunta_10 int,
    cod_adm_respuesta_10 int,
    ds_adm_area_10 string,
    ds_adm_preguntas_10 string,
    ds_adm_respuestas_10 string,
    cod_adm_area_11 int,
    cod_adm_pregunta_11 int,
    cod_adm_respuesta_11 int,
    ds_adm_area_11 string,
    ds_adm_preguntas_11 string,
    ds_adm_respuestas_11 string,
    cod_adm_area_12 int,
    cod_adm_pregunta_12 int,
    cod_adm_respuesta_12 int,
    ds_adm_area_12 string,
    ds_adm_preguntas_12 string,
    ds_adm_respuestas_12 string,
    cod_adm_area_13 int,
    cod_adm_pregunta_13 int,
    cod_adm_respuesta_13 int,
    ds_adm_area_13 string,
    ds_adm_preguntas_13 string,
    ds_adm_respuestas_13 string,
    cod_adm_area_14 int,
    cod_adm_pregunta_14 int,
    cod_adm_respuesta_14 int,
    ds_adm_area_14 string,
    ds_adm_preguntas_14 string,
    ds_adm_respuestas_14 string,
    cod_adm_area_15 int,
    cod_adm_pregunta_15 int,
    cod_adm_respuesta_15 int,
    ds_adm_area_15 string,
    ds_adm_preguntas_15 string,
    ds_adm_respuestas_15 string,
    cod_adm_area_16 int,
    cod_adm_pregunta_16 int,
    cod_adm_respuesta_16 int,
    ds_adm_area_16 string,
    ds_adm_preguntas_16 string,
    ds_adm_respuestas_16 string,
    cod_adm_area_17 int,
    cod_adm_pregunta_17 int,
    cod_adm_respuesta_17 int,
    ds_adm_area_17 string,
    ds_adm_preguntas_17 string,
    ds_adm_respuestas_17 string,
    cod_adm_area_18 int,
    cod_adm_pregunta_18 int,
    cod_adm_respuesta_18 int,
    ds_adm_area_18 string,
    ds_adm_preguntas_18 string,
    ds_adm_respuestas_18 string,
    cod_adm_area_19 int,
    cod_adm_pregunta_19 int,
    cod_adm_respuesta_19 int,
    ds_adm_area_19 string,
    ds_adm_preguntas_19 string,
    ds_adm_respuestas_19 string,
    cod_adm_area_20 int,
    cod_adm_pregunta_20 int,
    cod_adm_respuesta_20 int,
    ds_adm_area_20 string,
    ds_adm_preguntas_20 string,
    ds_adm_respuestas_20 string,
    cod_adm_area_21 int,
    cod_adm_pregunta_21 int,
    cod_adm_respuesta_21 int,
    ds_adm_area_21 string,
    ds_adm_preguntas_21 string,
    ds_adm_respuestas_21 string,
    cod_adm_area_22 int,
    cod_adm_pregunta_22 int,
    cod_adm_respuesta_22 int,
    ds_adm_area_22 string,
    ds_adm_preguntas_22 string,
    ds_adm_respuestas_22 string,
    cod_adm_area_23 int,
    cod_adm_pregunta_23 int,
    cod_adm_respuesta_23 int,
    ds_adm_area_23 string,
    ds_adm_preguntas_23 string,
    ds_adm_respuestas_23 string,
    cod_adm_area_24 int,
    cod_adm_pregunta_24 int,
    cod_adm_respuesta_24 int,
    ds_adm_area_24 string,
    ds_adm_preguntas_24 string,
    ds_adm_respuestas_24 string,
    cod_adm_area_25 int,
    cod_adm_pregunta_25 int,
    cod_adm_respuesta_25 int,
    ds_adm_area_25 string,
    ds_adm_preguntas_25 string,
    ds_adm_respuestas_25 string,
    cod_adm_area_26 int,
    cod_adm_pregunta_26 int,
    cod_adm_respuesta_26 int,
    ds_adm_area_26 string,
    ds_adm_preguntas_26 string,
    ds_adm_respuestas_26 string,
    dec_adm_rating_plus decimal(4, 2),
    flag_adm_tipo_rating_pre_final string,
    dec_adm_rating_pre_final decimal(4, 2),
    cod_adm_nro_prop bigint
)
PARTITIONED BY (
  partition_date string)
STORED AS PARQUET
LOCATION
  '${DATA_LAKE_SERVER}/santander/bi-corp/common/admision/stk_adm_empresas_rating_plus';