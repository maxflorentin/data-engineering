set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=4000000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.exec.dynamic.partition.mode=nonstrict;
set mapred.job.queue.name=root.dataeng;

DROP TABLE IF EXISTS cuestionario;
CREATE TEMPORARY TABLE cuestionario AS
select
    propuesta_cuestionario.nro_prop as numero_propuesta,
    propuesta_cuestionario.ree_cod_area as codigo_area,
    propuesta_cuestionario.ree_cod_area_pregunta as codigo_pregunta,
    propuesta_cuestionario.ree_cod_respuesta as codigo_respuesta,
    rating_areas.descripcion as descripcion_area,
    rating_preguntas.descripcion_pregunta as descripcion_preguntas,
    rating_respuestas.descripcion_respuesta as descripcion_respuestas,
    rank() over(partition by nro_prop order by ree_cod_area asc,ree_cod_area_pregunta asc) as orden
from bi_corp_staging.sge_valoracion_preguntas propuesta_cuestionario
inner join bi_corp_staging.sge_ree_areas rating_areas on rating_areas.codigo=propuesta_cuestionario.ree_cod_area and rating_areas.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Admision_Empresas-Daily') }}'
inner join bi_corp_staging.sge_ree_area_preguntas rating_preguntas on rating_preguntas.codigo=propuesta_cuestionario.ree_cod_area_pregunta and rating_preguntas.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Admision_Empresas-Daily') }}'
inner join bi_corp_staging.sge_ree_pregunta_respuestas rating_respuestas on rating_respuestas.codigo=propuesta_cuestionario.ree_cod_respuesta and rating_respuestas.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Admision_Empresas-Daily') }}'
where propuesta_cuestionario.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Admision_Empresas-Daily') }}' ;

DROP TABLE IF EXISTS temp;
CREATE TEMPORARY TABLE temp AS
select
    rc.nro_prop,
    v.cod_tipo_rating as codigo_tipo_rating,
    re_tipo_rating.descripcion as descripcion_tipo_rating,
    rdc.id_cuestionario,
    rdc.x_cuestionario,
    rdcp.cod_area,
    ra.descripcion as desc_area,
    rdcp.id_pregunta,
    rdcp.x_pregunta,
    rdcr.id_respuesta,
    rdcr.x_respuesta,
    rank() over(partition by rc.nro_prop order by rc.fi_ejecucion desc) as rank_calc,
    rank() over(partition by rc.nro_prop,rc.id_version order by ra.codigo, rdcp.id_pregunta asc) as rank_calc_preg
from bi_corp_staging.sge_valoracion v
inner join bi_corp_staging.sge_ree_tipos_rating re_tipo_rating on re_tipo_rating.codigo=v.cod_tipo_rating and re_tipo_rating.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Admision_Empresas-Daily') }}'
inner join bi_corp_staging.sge_rtg_calculos rc on v.nro_prop=rc.nro_prop and rc.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Admision_Empresas-Daily') }}'
inner join bi_corp_staging.sge_rtg_calculos_respuestas rcr on rcr.id_calculo=rc.id_calculo and rcr.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Admision_Empresas-Daily') }}'
inner join bi_corp_staging.sge_rtg_def_cuestionarios rdc on rdc.id_cuestionario=rcr.id_cuestionario and rdc.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Admision_Empresas-Daily') }}'
inner join bi_corp_staging.sge_rtg_def_cuest_preguntas rdcp on rdcp.id_cuestionario=rcr.id_cuestionario and rdcp.id_pregunta=rcr.id_pregunta and rdcp.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Admision_Empresas-Daily') }}'
inner join bi_corp_staging.sge_ree_areas ra on ra.codigo=rdcp.cod_area and ra.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Admision_Empresas-Daily') }}'
inner join bi_corp_staging.sge_rtg_def_cuest_respuestas rdcr on rdcr.id_pregunta=rcr.id_pregunta and rdcr.id_respuesta=rcr.id_respuesta and rdcr.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Admision_Empresas-Daily') }}'
where v.m_rating_plus='s' and v.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Admision_Empresas-Daily') }}';


DROP TABLE IF EXISTS re_preguntas_respuestas;
CREATE TEMPORARY TABLE re_preguntas_respuestas AS
select
   prop.nro_prop,
   val.cod_tipo_rating as codigo_tipo_rating,
   re_tipo_rating.descripcion as descripcion_tipo_rating,
   cuestionario.numero_propuesta,
   cuestionario.codigo_area,
   cuestionario.codigo_pregunta,
   cuestionario.codigo_respuesta,
   cuestionario.descripcion_area,
   cuestionario.descripcion_preguntas,
   cuestionario.descripcion_respuestas,
   cuestionario.orden
from bi_corp_staging.sge_propuesta prop
inner join bi_corp_staging.sge_valoracion val on val.nro_prop=prop.nro_prop and val.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Admision_Empresas-Daily') }}'
inner join bi_corp_staging.sge_ree_tipos_rating re_tipo_rating on re_tipo_rating.codigo=val.cod_tipo_rating and re_tipo_rating.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Admision_Empresas-Daily') }}'
inner join cuestionario on cuestionario.numero_propuesta=prop.nro_prop
where prop.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Admision_Empresas-Daily') }}'

union all

select
    nro_prop,
    codigo_tipo_rating,
    descripcion_tipo_rating,
    nro_prop as numero_propuesta,
    cod_area as codigo_area,
    id_pregunta as codigo_pregunta,
    id_respuesta as codigo_respuesta,
    desc_area as descripcion_area,
    x_pregunta as descripcion_preguntas,
    x_respuesta as descripcion_respuestas,
    rank_calc_preg as orden
from temp
where rank_calc=1;

DROP TABLE IF EXISTS temporal2;
CREATE TEMPORARY TABLE temporal2 AS
select
    cast(v.vcuali_pro_dem_mer_pun as decimal(4, 2)) as dec_adm_re_producto_demanda_mercado,
    cast(v.vcuali_acc_ger_pun as decimal(4, 2)) as dec_adm_re_accionistas_gerencia,
    cast(v.vcuali_acc_cred_pun as decimal(4, 2)) as dec_adm_re_acceso_credito,
    cast(v.vcuant_ben_rentab_pun as decimal(4, 2)) as dec_adm_re_beneficios_rentabilidad,
    cast(v.vcuant_gen_recursos_pun as decimal(3, 2)) as dec_adm_re_generacion_recursos,
    cast(v.vcuant_solvencia_pun as decimal(3, 2)) as dec_adm_re_solvencia,
    cast(v.rc_rating_matriz as decimal(16, 2)) as dec_adm_re_rating_matriz,
    cast(v.rc_prob_apoyo as int) as int_adm_re_probabilidad_apoyo_matriz,
    cast(v.vcuant_valoracion_final as decimal(3, 2)) as dec_adm_re_valoracion_final,
    cast(v.apoyo_casa_matriz as decimal(5, 2)) as dec_adm_re_apoyo_casa_matriz,
    cast(v.vcuant_val_estadistica as decimal(3, 2)) as dec_adm_re_valoracion_estadistica,
    case
        when coalesce(v.vcuant_valoracion_final,0)>0 then v.vcuant_valoracion_final
        when coalesce(v.vcuant_val_estadistica,0)>0 then v.vcuant_val_estadistica
        when coalesce(v.apoyo_casa_matriz,0)>0 then v.apoyo_casa_matriz
        else '0'
    end as ds_adm_rating_experto,
    cast(v.ree_rating_estadistico as decimal(16, 2)) as dec_adm_ree_rating_estadistico,
    cast(v.ree_rating_apoyo_matriz as decimal(16, 2)) as dec_adm_ree_rating_apoyo_matriz,
    cast(v.ree_rating_est_experto as decimal(16, 2)) as dec_adm_ree_rating_estadistico_experto,
    case
        when coalesce(cast(v.ree_rating_estadistico as decimal(16, 2)),0)>0 then cast(v.ree_rating_estadistico as decimal(16, 2))
        when coalesce(cast(v.ree_rating_est_experto as decimal(16, 2)),0)>0 then cast(v.ree_rating_est_experto as decimal(16, 2))
        when coalesce(cast(v.ree_rating_apoyo_matriz as decimal(16, 2)),0)>0 then cast(v.ree_rating_apoyo_matriz as decimal(16, 2))
        else 0
    end as dec_adm_rating_estadistico_experto,

    v.ree_motivo_no_aplic as ds_adm_ree_motivo_no_aplica,

    case
        when v.ree_motivos_desacuerdo=1 then 'evolución de mercado'
        when v.ree_motivos_desacuerdo=2 then 'accionistas (cambio esperado/recomposicion)'
        when v.ree_motivos_desacuerdo=3 then 'proyecciones eco/financ/pca'
        when v.ree_motivos_desacuerdo=4 then 'comportamiento'
        else null
    end as ds_adm_ree_motivos_desacuerdo,

    v.fecha_valoracion as dt_adm_valoracion,
    cast(v.ree_id_balance_1 as int) as id_adm_ree_balance_1,
    cast(v.ree_id_balance_2 as int) as id_adm_ree_balance_2,

    concat(coalesce(v.obs_acc_ger,''), coalesce(v.obs_acc_ger_2,' ')) as ds_adm_obs_acc_ger,
    concat(coalesce(v.obs_pro_dem_mer,''), coalesce(v.obs_pro_dem_mer_2,' '), coalesce(v.obs_pro_dem_mer_3,' '), coalesce(v.obs_pro_dem_mer_4,' ')) as ds_adm_obs_pro_dem_mer,
    concat(coalesce(v.obs_acc_cred,''), coalesce(v.obs_acc_cred_2,' ')) as ds_adm_obs_acc_cred,
    concat(coalesce(v.obs_ben_rentab,''), coalesce(v.obs_ben_rentab_2,' ')) as ds_adm_obs_ben_rentab,
    concat(coalesce(v.obs_gen_recursos,''), coalesce(v.obs_gen_recursos_2,' ')) as ds_adm_obs_gen_recursos,
    concat(coalesce(v.obs_solvencia,''), coalesce(v.obs_solvencia_2,' ')) as ds_adm_obs_solvencia,
    concat(coalesce(v.obs_proyecciones,''), coalesce(v.obs_proyecciones_2,' ')) as ds_adm_obs_proyecciones,
    concat(coalesce(v.obs_exp_ant,''), coalesce(v.obs_exp_ant_2,' ')) as ds_adm_obs_exp_ant,
    concat(coalesce(v.obs_sintesis,''), coalesce(v.obs_sintesis_2,' ')) as ds_adm_obs_sintesis,
    coalesce(v.resumen_act,'') as ds_adm_resumen_act,
    coalesce(v.bullet_points,'') as ds_adm_bullet_points,
    coalesce(v.comentarios,'') as ds_adm_comentarios,
    coalesce(v.riesgos,'') as ds_adm_riesgos,
    coalesce(v.mitigantes,'') as ds_adm_mitigantes,

    date_format(v.fecha_valoracion, 'yyyyMM') as ds_adm_periodo,
    cast(pg.codigo_tipo_rating as int) as cod_adm_tipo_rating,
    pg.descripcion_tipo_rating as ds_adm_tipo_rating,

    case when pg.orden = 1 then pg.codigo_area end as cod_adm_area_1,
    case when pg.orden = 1 then pg.codigo_pregunta end as cod_adm_pregunta_1,
    case when pg.orden = 1 then pg.codigo_respuesta end as cod_adm_respuesta_1,
    case when pg.orden = 1 then pg.descripcion_area end as ds_adm_area_1,
    case when pg.orden = 1 then pg.descripcion_preguntas end as ds_adm_preguntas_1,
    case when pg.orden = 1 then pg.descripcion_respuestas end as ds_adm_respuestas_1,

    case when pg.orden = 2 then pg.codigo_area end as cod_adm_area_2,
    case when pg.orden = 2 then pg.codigo_pregunta end as cod_adm_pregunta_2,
    case when pg.orden = 2 then pg.codigo_respuesta end as cod_adm_respuesta_2,
    case when pg.orden = 2 then pg.descripcion_area end as ds_adm_area_2,
    case when pg.orden = 2 then pg.descripcion_preguntas end as ds_adm_preguntas_2,
    case when pg.orden = 2 then pg.descripcion_respuestas end as ds_adm_respuestas_2,

    case when pg.orden = 3 then pg.codigo_area end as cod_adm_area_3,
    case when pg.orden = 3 then pg.codigo_pregunta end as cod_adm_pregunta_3,
    case when pg.orden = 3 then pg.codigo_respuesta end as cod_adm_respuesta_3,
    case when pg.orden = 3 then pg.descripcion_area end as ds_adm_area_3,
    case when pg.orden = 3 then pg.descripcion_preguntas end as ds_adm_preguntas_3,
    case when pg.orden = 3 then pg.descripcion_respuestas end as ds_adm_respuestas_3,

    case when pg.orden = 4 then pg.codigo_area end as cod_adm_area_4,
    case when pg.orden = 4 then pg.codigo_pregunta end as cod_adm_pregunta_4,
    case when pg.orden = 4 then pg.codigo_respuesta end as cod_adm_respuesta_4,
    case when pg.orden = 4 then pg.descripcion_area end as ds_adm_area_4,
    case when pg.orden = 4 then pg.descripcion_preguntas end as ds_adm_preguntas_4,
    case when pg.orden = 4 then pg.descripcion_respuestas end as ds_adm_respuestas_4,

    case when pg.orden = 5 then pg.codigo_area end as cod_adm_area_5,
    case when pg.orden = 5 then pg.codigo_pregunta end as cod_adm_pregunta_5,
    case when pg.orden = 5 then pg.codigo_respuesta end as cod_adm_respuesta_5,
    case when pg.orden = 5 then pg.descripcion_area end as ds_adm_area_5,
    case when pg.orden = 5 then pg.descripcion_preguntas end as ds_adm_preguntas_5,
    case when pg.orden = 5 then pg.descripcion_respuestas end as ds_adm_respuestas_5,

    case when pg.orden = 6 then pg.codigo_area end as cod_adm_area_6,
    case when pg.orden = 6 then pg.codigo_pregunta end as cod_adm_pregunta_6,
    case when pg.orden = 6 then pg.codigo_respuesta end as cod_adm_respuesta_6,
    case when pg.orden = 6 then pg.descripcion_area end as ds_adm_area_6,
    case when pg.orden = 6 then pg.descripcion_preguntas end as ds_adm_preguntas_6,
    case when pg.orden = 6 then pg.descripcion_respuestas end as ds_adm_respuestas_6,

    case when pg.orden = 7 then pg.codigo_area end as cod_adm_area_7,
    case when pg.orden = 7 then pg.codigo_pregunta end as cod_adm_pregunta_7,
    case when pg.orden = 7 then pg.codigo_respuesta end as cod_adm_respuesta_7,
    case when pg.orden = 7 then pg.descripcion_area end as ds_adm_area_7,
    case when pg.orden = 7 then pg.descripcion_preguntas end as ds_adm_preguntas_7,
    case when pg.orden = 7 then pg.descripcion_respuestas end as ds_adm_respuestas_7,

    case when pg.orden = 8 then pg.codigo_area end as cod_adm_area_8,
    case when pg.orden = 8 then pg.codigo_pregunta end as cod_adm_pregunta_8,
    case when pg.orden = 8 then pg.codigo_respuesta end as cod_adm_respuesta_8,
    case when pg.orden = 8 then pg.descripcion_area end as ds_adm_area_8,
    case when pg.orden = 8 then pg.descripcion_preguntas end as ds_adm_preguntas_8,
    case when pg.orden = 8 then pg.descripcion_respuestas end as ds_adm_respuestas_8,

    case when pg.orden = 9 then pg.codigo_area end as cod_adm_area_9,
    case when pg.orden = 9 then pg.codigo_pregunta end as cod_adm_pregunta_9,
    case when pg.orden = 9 then pg.codigo_respuesta end as cod_adm_respuesta_9,
    case when pg.orden = 9 then pg.descripcion_area end as ds_adm_area_9,
    case when pg.orden = 9 then pg.descripcion_preguntas end as ds_adm_preguntas_9,
    case when pg.orden = 9 then pg.descripcion_respuestas end as ds_adm_respuestas_9,

    case when pg.orden = 10 then pg.codigo_area end as cod_adm_area_10,
    case when pg.orden = 10 then pg.codigo_pregunta end as cod_adm_pregunta_10,
    case when pg.orden = 10 then pg.codigo_respuesta end as cod_adm_respuesta_10,
    case when pg.orden = 10 then pg.descripcion_area end as ds_adm_area_10,
    case when pg.orden = 10 then pg.descripcion_preguntas end as ds_adm_preguntas_10,
    case when pg.orden = 10 then pg.descripcion_respuestas end as ds_adm_respuestas_10,

    case when pg.orden = 11 then pg.codigo_area end as cod_adm_area_11,
    case when pg.orden = 11 then pg.codigo_pregunta end as cod_adm_pregunta_11,
    case when pg.orden = 11 then pg.codigo_respuesta end as cod_adm_respuesta_11,
    case when pg.orden = 11 then pg.descripcion_area end as ds_adm_area_11,
    case when pg.orden = 11 then pg.descripcion_preguntas end as ds_adm_preguntas_11,
    case when pg.orden = 11 then pg.descripcion_respuestas end as ds_adm_respuestas_11,

    case when pg.orden = 12 then pg.codigo_area end as cod_adm_area_12,
    case when pg.orden = 12 then pg.codigo_pregunta end as cod_adm_pregunta_12,
    case when pg.orden = 12 then pg.codigo_respuesta end as cod_adm_respuesta_12,
    case when pg.orden = 12 then pg.descripcion_area end as ds_adm_area_12,
    case when pg.orden = 12 then pg.descripcion_preguntas end as ds_adm_preguntas_12,
    case when pg.orden = 12 then pg.descripcion_respuestas end as ds_adm_respuestas_12,

    case when pg.orden = 13 then pg.codigo_area end as cod_adm_area_13,
    case when pg.orden = 13 then pg.codigo_pregunta end as cod_adm_pregunta_13,
    case when pg.orden = 13 then pg.codigo_respuesta end as cod_adm_respuesta_13,
    case when pg.orden = 13 then pg.descripcion_area end as ds_adm_area_13,
    case when pg.orden = 13 then pg.descripcion_preguntas end as ds_adm_preguntas_13,
    case when pg.orden = 13 then pg.descripcion_respuestas end as ds_adm_respuestas_13,

    case when pg.orden = 14 then pg.codigo_area end as cod_adm_area_14,
    case when pg.orden = 14 then pg.codigo_pregunta end as cod_adm_pregunta_14,
    case when pg.orden = 14 then pg.codigo_respuesta end as cod_adm_respuesta_14,
    case when pg.orden = 14 then pg.descripcion_area end as ds_adm_area_14,
    case when pg.orden = 14 then pg.descripcion_preguntas end as ds_adm_preguntas_14,
    case when pg.orden = 14 then pg.descripcion_respuestas end as ds_adm_respuestas_14,

    case when pg.orden = 15 then pg.codigo_area end as cod_adm_area_15,
    case when pg.orden = 15 then pg.codigo_pregunta end as cod_adm_pregunta_15,
    case when pg.orden = 15 then pg.codigo_respuesta end as cod_adm_respuesta_15,
    case when pg.orden = 15 then pg.descripcion_area end as ds_adm_area_15,
    case when pg.orden = 15 then pg.descripcion_preguntas end as ds_adm_preguntas_15,
    case when pg.orden = 15 then pg.descripcion_respuestas end as ds_adm_respuestas_15,

    case when pg.orden = 16 then pg.codigo_area end as cod_adm_area_16,
    case when pg.orden = 16 then pg.codigo_pregunta end as cod_adm_pregunta_16,
    case when pg.orden = 16 then pg.codigo_respuesta end as cod_adm_respuesta_16,
    case when pg.orden = 16 then pg.descripcion_area end as ds_adm_area_16,
    case when pg.orden = 16 then pg.descripcion_preguntas end as ds_adm_preguntas_16,
    case when pg.orden = 16 then pg.descripcion_respuestas end as ds_adm_respuestas_16,

    case when pg.orden = 17 then pg.codigo_area end as cod_adm_area_17,
    case when pg.orden = 17 then pg.codigo_pregunta end as cod_adm_pregunta_17,
    case when pg.orden = 17 then pg.codigo_respuesta end as cod_adm_respuesta_17,
    case when pg.orden = 17 then pg.descripcion_area end as ds_adm_area_17,
    case when pg.orden = 17 then pg.descripcion_preguntas end as ds_adm_preguntas_17,
    case when pg.orden = 17 then pg.descripcion_respuestas end as ds_adm_respuestas_17,

    case when pg.orden = 18 then pg.codigo_area end as cod_adm_area_18,
    case when pg.orden = 18 then pg.codigo_pregunta end as cod_adm_pregunta_18,
    case when pg.orden = 18 then pg.codigo_respuesta end as cod_adm_respuesta_18,
    case when pg.orden = 18 then pg.descripcion_area end as ds_adm_area_18,
    case when pg.orden = 18 then pg.descripcion_preguntas end as ds_adm_preguntas_18,
    case when pg.orden = 18 then pg.descripcion_respuestas end as ds_adm_respuestas_18,

    case when pg.orden = 19 then pg.codigo_area end as cod_adm_area_19,
    case when pg.orden = 19 then pg.codigo_pregunta end as cod_adm_pregunta_19,
    case when pg.orden = 19 then pg.codigo_respuesta end as cod_adm_respuesta_19,
    case when pg.orden = 19 then pg.descripcion_area end as ds_adm_area_19,
    case when pg.orden = 19 then pg.descripcion_preguntas end as ds_adm_preguntas_19,
    case when pg.orden = 19 then pg.descripcion_respuestas end as ds_adm_respuestas_19,

    case when pg.orden = 20 then pg.codigo_area end as cod_adm_area_20,
    case when pg.orden = 20 then pg.codigo_pregunta end as cod_adm_pregunta_20,
    case when pg.orden = 20 then pg.codigo_respuesta end as cod_adm_respuesta_20,
    case when pg.orden = 20 then pg.descripcion_area end as ds_adm_area_20,
    case when pg.orden = 20 then pg.descripcion_preguntas end as ds_adm_preguntas_20,
    case when pg.orden = 20 then pg.descripcion_respuestas end as ds_adm_respuestas_20,

    case when pg.orden = 21 then pg.codigo_area end as cod_adm_area_21,
    case when pg.orden = 21 then pg.codigo_pregunta end as cod_adm_pregunta_21,
    case when pg.orden = 21 then pg.codigo_respuesta end as cod_adm_respuesta_21,
    case when pg.orden = 21 then pg.descripcion_area end as ds_adm_area_21,
    case when pg.orden = 21 then pg.descripcion_preguntas end as ds_adm_preguntas_21,
    case when pg.orden = 21 then pg.descripcion_respuestas end as ds_adm_respuestas_21,

    case when pg.orden = 22 then pg.codigo_area end as cod_adm_area_22,
    case when pg.orden = 22 then pg.codigo_pregunta end as cod_adm_pregunta_22,
    case when pg.orden = 22 then pg.codigo_respuesta end as cod_adm_respuesta_22,
    case when pg.orden = 22 then pg.descripcion_area end as ds_adm_area_22,
    case when pg.orden = 22 then pg.descripcion_preguntas end as ds_adm_preguntas_22,
    case when pg.orden = 22 then pg.descripcion_respuestas end as ds_adm_respuestas_22,

    case when pg.orden = 23 then pg.codigo_area end as cod_adm_area_23,
    case when pg.orden = 23 then pg.codigo_pregunta end as cod_adm_pregunta_23,
    case when pg.orden = 23 then pg.codigo_respuesta end as cod_adm_respuesta_23,
    case when pg.orden = 23 then pg.descripcion_area end as ds_adm_area_23,
    case when pg.orden = 23 then pg.descripcion_preguntas end as ds_adm_preguntas_23,
    case when pg.orden = 23 then pg.descripcion_respuestas end as ds_adm_respuestas_23,

    case when pg.orden = 24 then pg.codigo_area end as cod_adm_area_24,
    case when pg.orden = 24 then pg.codigo_pregunta end as cod_adm_pregunta_24,
    case when pg.orden = 24 then pg.codigo_respuesta end as cod_adm_respuesta_24,
    case when pg.orden = 24 then pg.descripcion_area end as ds_adm_area_24,
    case when pg.orden = 24 then pg.descripcion_preguntas end as ds_adm_preguntas_24,
    case when pg.orden = 24 then pg.descripcion_respuestas end as ds_adm_respuestas_24,

    case when pg.orden = 25 then pg.codigo_area end as cod_adm_area_25,
    case when pg.orden = 25 then pg.codigo_pregunta end as cod_adm_pregunta_25,
    case when pg.orden = 25 then pg.codigo_respuesta end as cod_adm_respuesta_25,
    case when pg.orden = 25 then pg.descripcion_area end as ds_adm_area_25,
    case when pg.orden = 25 then pg.descripcion_preguntas end as ds_adm_preguntas_25,
    case when pg.orden = 25 then pg.descripcion_respuestas end as ds_adm_respuestas_25,

    case when pg.orden = 26 then pg.codigo_area end as cod_adm_area_26,
    case when pg.orden = 26 then pg.codigo_pregunta end as cod_adm_pregunta_26,
    case when pg.orden = 26 then pg.codigo_respuesta end as cod_adm_respuesta_26,
    case when pg.orden = 26 then pg.descripcion_area end as ds_adm_area_26,
    case when pg.orden = 26 then pg.descripcion_preguntas end as ds_adm_preguntas_26,
    case when pg.orden = 26 then pg.descripcion_respuestas end as ds_adm_respuestas_26,

    cast(v.rating_plus as decimal(4, 2)) as dec_adm_rating_plus,
    v.t_rating_final as flag_adm_tipo_rating_pre_final,
    v.rating_final as dec_adm_rating_pre_final,
    cast(v.nro_prop as bigint) as cod_adm_nro_prop
from bi_corp_staging.sge_valoracion v
left join re_preguntas_respuestas pg on pg.nro_prop=v.nro_prop
where v.nro_prop is not null and v.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Admision_Empresas-Daily') }}';


INSERT OVERWRITE TABLE bi_corp_common.stk_adm_empresas_rating_plus
    PARTITION (partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Admision_Empresas-Daily') }}')
select
    max(dec_adm_re_producto_demanda_mercado) as dec_adm_re_producto_demanda_mercado,
    max(dec_adm_re_accionistas_gerencia) as dec_adm_re_accionistas_gerencia,
    max(dec_adm_re_acceso_credito) as dec_adm_re_acceso_credito,
    max(dec_adm_re_beneficios_rentabilidad) as dec_adm_re_beneficios_rentabilidad,
    max(dec_adm_re_generacion_recursos) as dec_adm_re_generacion_recursos,
    max(dec_adm_re_solvencia) as dec_adm_re_solvencia,
    max(dec_adm_re_rating_matriz) as dec_adm_re_rating_matriz,
    max(int_adm_re_probabilidad_apoyo_matriz) as int_adm_re_probabilidad_apoyo_matriz,
    max(dec_adm_re_valoracion_final) as dec_adm_re_valoracion_final,
    max(dec_adm_re_apoyo_casa_matriz) as dec_adm_re_apoyo_casa_matriz,
    max(dec_adm_re_valoracion_estadistica) as dec_adm_re_valoracion_estadistica,
    max(ds_adm_rating_experto) as ds_adm_rating_experto,
    max(dec_adm_ree_rating_estadistico) as dec_adm_ree_rating_estadistico,
    max(dec_adm_ree_rating_apoyo_matriz) as dec_adm_ree_rating_apoyo_matriz,
    max(dec_adm_ree_rating_estadistico_experto) as dec_adm_ree_rating_estadistico_experto,
    max(dec_adm_rating_estadistico_experto) as dec_adm_rating_estadistico_experto,
    max(ds_adm_ree_motivo_no_aplica) as ds_adm_ree_motivo_no_aplica,
    max(ds_adm_ree_motivos_desacuerdo) as ds_adm_ree_motivos_desacuerdo,
    max(dt_adm_valoracion) as dt_adm_valoracion,
    max(id_adm_ree_balance_1) as id_adm_ree_balance_1,
    max(id_adm_ree_balance_2) as id_adm_ree_balance_2,
    max(ds_adm_obs_acc_ger) as ds_adm_obs_acc_ger,
    max(ds_adm_obs_pro_dem_mer) as ds_adm_obs_pro_dem_mer,
    max(ds_adm_obs_acc_cred) as ds_adm_obs_acc_cred,
    max(ds_adm_obs_ben_rentab) as ds_adm_obs_ben_rentab,
    max(ds_adm_obs_gen_recursos) as ds_adm_obs_gen_recursos,
    max(ds_adm_obs_solvencia) as ds_adm_obs_solvencia,
    max(ds_adm_obs_proyecciones) as ds_adm_obs_proyecciones,
    max(ds_adm_obs_exp_ant) as ds_adm_obs_exp_ant,
    max(ds_adm_obs_sintesis) as ds_adm_obs_sintesis,
    max(ds_adm_resumen_act) as ds_adm_resumen_act,
    max(ds_adm_bullet_points) as ds_adm_bullet_points,
    max(ds_adm_comentarios) as ds_adm_comentarios,
    max(ds_adm_riesgos) as ds_adm_riesgos,
    max(ds_adm_mitigantes) as ds_adm_mitigantes,
    max(ds_adm_periodo) as ds_adm_periodo,
    max(cod_adm_tipo_rating) as cod_adm_tipo_rating,
    max(ds_adm_tipo_rating) as ds_adm_tipo_rating,
    max(cod_adm_area_1) as cod_adm_area_1,
    max(cod_adm_pregunta_1) as cod_adm_pregunta_1,
    max(cod_adm_respuesta_1) as cod_adm_respuesta_1,
    max(ds_adm_area_1) as ds_adm_area_1,
    max(ds_adm_preguntas_1) as ds_adm_preguntas_1,
    max(ds_adm_respuestas_1) as ds_adm_respuestas_1,
    max(cod_adm_area_2) as cod_adm_area_2,
    max(cod_adm_pregunta_2) as cod_adm_pregunta_2,
    max(cod_adm_respuesta_2) as cod_adm_respuesta_2,
    max(ds_adm_area_2) as ds_adm_area_2,
    max(ds_adm_preguntas_2) as ds_adm_preguntas_2,
    max(ds_adm_respuestas_2) as ds_adm_respuestas_2,
    max(cod_adm_area_3) as cod_adm_area_3,
    max(cod_adm_pregunta_3) as cod_adm_pregunta_3,
    max(cod_adm_respuesta_3) as cod_adm_respuesta_3,
    max(ds_adm_area_3) as ds_adm_area_3,
    max(ds_adm_preguntas_3) as ds_adm_preguntas_3,
    max(ds_adm_respuestas_3) as ds_adm_respuestas_3,
    max(cod_adm_area_4) as cod_adm_area_4,
    max(cod_adm_pregunta_4) as cod_adm_pregunta_4,
    max(cod_adm_respuesta_4) as cod_adm_respuesta_4,
    max(ds_adm_area_4) as ds_adm_area_4,
    max(ds_adm_preguntas_4) as ds_adm_preguntas_4,
    max(ds_adm_respuestas_4) as ds_adm_respuestas_4,
    max(cod_adm_area_5) as cod_adm_area_5,
    max(cod_adm_pregunta_5) as cod_adm_pregunta_5,
    max(cod_adm_respuesta_5) as cod_adm_respuesta_5,
    max(ds_adm_area_5) as ds_adm_area_5,
    max(ds_adm_preguntas_5) as ds_adm_preguntas_5,
    max(ds_adm_respuestas_5) as ds_adm_respuestas_5,
    max(cod_adm_area_6) as cod_adm_area_6,
    max(cod_adm_pregunta_6) as cod_adm_pregunta_6,
    max(cod_adm_respuesta_6) as cod_adm_respuesta_6,
    max(ds_adm_area_6) as ds_adm_area_6,
    max(ds_adm_preguntas_6) as ds_adm_preguntas_6,
    max(ds_adm_respuestas_6) as ds_adm_respuestas_6,
    max(cod_adm_area_7) as cod_adm_area_7,
    max(cod_adm_pregunta_7) as cod_adm_pregunta_7,
    max(cod_adm_respuesta_7) as cod_adm_respuesta_7,
    max(ds_adm_area_7) as ds_adm_area_7,
    max(ds_adm_preguntas_7) as ds_adm_preguntas_7,
    max(ds_adm_respuestas_7) as ds_adm_respuestas_7,
    max(cod_adm_area_8) as cod_adm_area_8,
    max(cod_adm_pregunta_8) as cod_adm_pregunta_8,
    max(cod_adm_respuesta_8) as cod_adm_respuesta_8,
    max(ds_adm_area_8) as ds_adm_area_8,
    max(ds_adm_preguntas_8) as ds_adm_preguntas_8,
    max(ds_adm_respuestas_8) as ds_adm_respuestas_8,
    max(cod_adm_area_9) as cod_adm_area_9,
    max(cod_adm_pregunta_9) as cod_adm_pregunta_9,
    max(cod_adm_respuesta_9) as cod_adm_respuesta_9,
    max(ds_adm_area_9) as ds_adm_area_9,
    max(ds_adm_preguntas_9) as ds_adm_preguntas_9,
    max(ds_adm_respuestas_9) as ds_adm_respuestas_9,
    max(cod_adm_area_10) as cod_adm_area_10,
    max(cod_adm_pregunta_10) as cod_adm_pregunta_10,
    max(cod_adm_respuesta_10) as cod_adm_respuesta_10,
    max(ds_adm_area_10) as ds_adm_area_10,
    max(ds_adm_preguntas_10) as ds_adm_preguntas_10,
    max(ds_adm_respuestas_10) as ds_adm_respuestas_10,
    max(cod_adm_area_11) as cod_adm_area_11,
    max(cod_adm_pregunta_11) as cod_adm_pregunta_11,
    max(cod_adm_respuesta_11) as cod_adm_respuesta_11,
    max(ds_adm_area_11) as ds_adm_area_11,
    max(ds_adm_preguntas_11) as ds_adm_preguntas_11,
    max(ds_adm_respuestas_11) as ds_adm_respuestas_11,
    max(cod_adm_area_12) as cod_adm_area_12,
    max(cod_adm_pregunta_12) as cod_adm_pregunta_12,
    max(cod_adm_respuesta_12) as cod_adm_respuesta_12,
    max(ds_adm_area_12) as ds_adm_area_12,
    max(ds_adm_preguntas_12) as ds_adm_preguntas_12,
    max(ds_adm_respuestas_12) as ds_adm_respuestas_12,
    max(cod_adm_area_13) as cod_adm_area_13,
    max(cod_adm_pregunta_13) as cod_adm_pregunta_13,
    max(cod_adm_respuesta_13) as cod_adm_respuesta_13,
    max(ds_adm_area_13) as ds_adm_area_13,
    max(ds_adm_preguntas_13) as ds_adm_preguntas_13,
    max(ds_adm_respuestas_13) as ds_adm_respuestas_13,
    max(cod_adm_area_14) as cod_adm_area_14,
    max(cod_adm_pregunta_14) as cod_adm_pregunta_14,
    max(cod_adm_respuesta_14) as cod_adm_respuesta_14,
    max(ds_adm_area_14) as ds_adm_area_14,
    max(ds_adm_preguntas_14) as ds_adm_preguntas_14,
    max(ds_adm_respuestas_14) as ds_adm_respuestas_14,
    max(cod_adm_area_15) as cod_adm_area_15,
    max(cod_adm_pregunta_15) as cod_adm_pregunta_15,
    max(cod_adm_respuesta_15) as cod_adm_respuesta_15,
    max(ds_adm_area_15) as ds_adm_area_15,
    max(ds_adm_preguntas_15) as ds_adm_preguntas_15,
    max(ds_adm_respuestas_15) as ds_adm_respuestas_15,
    max(cod_adm_area_16) as cod_adm_area_16,
    max(cod_adm_pregunta_16) as cod_adm_pregunta_16,
    max(cod_adm_respuesta_16) as cod_adm_respuesta_16,
    max(ds_adm_area_16) as ds_adm_area_16,
    max(ds_adm_preguntas_16) as ds_adm_preguntas_16,
    max(ds_adm_respuestas_16) as ds_adm_respuestas_16,
    max(cod_adm_area_17) as cod_adm_area_17,
    max(cod_adm_pregunta_17) as cod_adm_pregunta_17,
    max(cod_adm_respuesta_17) as cod_adm_respuesta_17,
    max(ds_adm_area_17) as ds_adm_area_17,
    max(ds_adm_preguntas_17) as ds_adm_preguntas_17,
    max(ds_adm_respuestas_17) as ds_adm_respuestas_17,
    max(cod_adm_area_18) as cod_adm_area_18,
    max(cod_adm_pregunta_18) as cod_adm_pregunta_18,
    max(cod_adm_respuesta_18) as cod_adm_respuesta_18,
    max(ds_adm_area_18) as ds_adm_area_18,
    max(ds_adm_preguntas_18) as ds_adm_preguntas_18,
    max(ds_adm_respuestas_18) as ds_adm_respuestas_18,
    max(cod_adm_area_19) as cod_adm_area_19,
    max(cod_adm_pregunta_19) as cod_adm_pregunta_19,
    max(cod_adm_respuesta_19) as cod_adm_respuesta_19,
    max(ds_adm_area_19) as ds_adm_area_19,
    max(ds_adm_preguntas_19) as ds_adm_preguntas_19,
    max(ds_adm_respuestas_19) as ds_adm_respuestas_19,
    max(cod_adm_area_20) as cod_adm_area_20,
    max(cod_adm_pregunta_20) as cod_adm_pregunta_20,
    max(cod_adm_respuesta_20) as cod_adm_respuesta_20,
    max(ds_adm_area_20) as ds_adm_area_20,
    max(ds_adm_preguntas_20) as ds_adm_preguntas_20,
    max(ds_adm_respuestas_20) as ds_adm_respuestas_20,
    max(cod_adm_area_21) as cod_adm_area_21,
    max(cod_adm_pregunta_21) as cod_adm_pregunta_21,
    max(cod_adm_respuesta_21) as cod_adm_respuesta_21,
    max(ds_adm_area_21) as ds_adm_area_21,
    max(ds_adm_preguntas_21) as ds_adm_preguntas_21,
    max(ds_adm_respuestas_21) as ds_adm_respuestas_21,
    max(cod_adm_area_22) as cod_adm_area_22,
    max(cod_adm_pregunta_22) as cod_adm_pregunta_22,
    max(cod_adm_respuesta_22) as cod_adm_respuesta_22,
    max(ds_adm_area_22) as ds_adm_area_22,
    max(ds_adm_preguntas_22) as ds_adm_preguntas_22,
    max(ds_adm_respuestas_22) as ds_adm_respuestas_22,
    max(cod_adm_area_23) as cod_adm_area_23,
    max(cod_adm_pregunta_23) as cod_adm_pregunta_23,
    max(cod_adm_respuesta_23) as cod_adm_respuesta_23,
    max(ds_adm_area_23) as ds_adm_area_23,
    max(ds_adm_preguntas_23) as ds_adm_preguntas_23,
    max(ds_adm_respuestas_23) as ds_adm_respuestas_23,
    max(cod_adm_area_24) as cod_adm_area_24,
    max(cod_adm_pregunta_24) as cod_adm_pregunta_24,
    max(cod_adm_respuesta_24) as cod_adm_respuesta_24,
    max(ds_adm_area_24) as ds_adm_area_24,
    max(ds_adm_preguntas_24) as ds_adm_preguntas_24,
    max(ds_adm_respuestas_24) as ds_adm_respuestas_24,
    max(cod_adm_area_25) as cod_adm_area_25,
    max(cod_adm_pregunta_25) as cod_adm_pregunta_25,
    max(cod_adm_respuesta_25) as cod_adm_respuesta_25,
    max(ds_adm_area_25) as ds_adm_area_25,
    max(ds_adm_preguntas_25) as ds_adm_preguntas_25,
    max(ds_adm_respuestas_25) as ds_adm_respuestas_25,
    max(cod_adm_area_26) as cod_adm_area_26,
    max(cod_adm_pregunta_26) as cod_adm_pregunta_26,
    max(cod_adm_respuesta_26) as cod_adm_respuesta_26,
    max(ds_adm_area_26) as ds_adm_area_26,
    max(ds_adm_preguntas_26) as ds_adm_preguntas_26,
    max(ds_adm_respuestas_26) as ds_adm_respuestas_26,
    max(dec_adm_rating_plus) as dec_adm_rating_plus,
    max(flag_adm_tipo_rating_pre_final) as flag_adm_tipo_rating_pre_final,
    max(dec_adm_rating_pre_final) as dec_adm_rating_pre_final,
    cod_adm_nro_prop
from temporal2
group by cod_adm_nro_prop;