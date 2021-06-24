set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=4000000;
set hive.merge.smallfiles.avgsize=16000000;
set mapred.job.queue.name=root.dataeng;

insert overwrite table bi_corp_common.dim_gec_ofertas_contenido
select
                case when trim(id_oferta_rtd)='null' then null else trim(id_oferta_rtd) end cod_gec_oferta,
                case when trim(id_canal)='null' then null else trim(id_canal) end cod_gec_canal,
                B.ds_gec_canal,
                case when trim(accion_final)='null' then null else trim(accion_final) end ds_gec_ult_accion,
                case when trim(periodo_descanso)='null' then null else trim(periodo_descanso) end cod_gec_periodo_descanso,
                case when trim(periodo_presentaciones_1)='null' then null else trim(periodo_presentaciones_1) end cod_gec_periodo_presentaciones1,
                case when trim(limite_presentaciones_1)='null' then null else trim(limite_presentaciones_1) end cod_gec_limite_presentaciones1,
                case when trim(periodo_presentaciones_2)='null' then null else trim(periodo_presentaciones_2) end cod_gec_periodo_presentaciones2,
                case when trim(limite_presentaciones_2)='null' then null else trim(limite_presentaciones_2) end cod_gec_limite_presentaciones2,
                case when trim(periodo_presentaciones_3)='null' then null else trim(periodo_presentaciones_3) end cod_gec_periodo_presentaciones3,
                case when trim(limite_presentaciones_3)='null' then null else trim(limite_presentaciones_3) end cod_gec_limite_presentaciones3,
                case when trim(parametro_1_char)='null' then null else trim(parametro_1_char) end cod_gec_parametro_1,
                case when trim(parametro_1_num)='null' then null else trim(parametro_1_num) end cod_gec_parametro_1num,
                case when trim(parametro_2_char)='null' then null else trim(parametro_2_char) end cod_gec_parametro_2,
                case when trim(parametro_2_num)='null' then null else trim(parametro_2_num) end cod_gec_parametro_2num,
                case when trim(parametro_3_char)='null' then null else trim(parametro_3_char) end cod_gec_parametro_3,
                case when trim(parametro_3_num)='null' then null else trim(parametro_3_num) end cod_gec_parametro_3num,
                case when trim(parametro_4_char)='null' then null else trim(parametro_4_char) end cod_gec_parametro_4,
                case when trim(parametro_4_num)='null' then null else trim(parametro_4_num) end cod_gec_parametro_4num,
                case when trim(parametro_5_char)='null' then null else trim(parametro_5_char) end cod_gec_parametro_5,
                case when trim(parametro_5_num)='null' then null else trim(parametro_5_num) end cod_gec_parametro_5num,
                case when trim(parametro_6_char)='null' then null else trim(parametro_6_char) end cod_gec_parametro_6,
                case when trim(parametro_6_num)='null' then null else trim(parametro_6_num) end cod_gec_parametro_6num,
                case when trim(parametro_7_char)='null' then null else trim(parametro_7_char) end cod_gec_parametro_7,
                case when trim(parametro_7_num)='null' then null else trim(parametro_7_num) end cod_gec_parametro_7num,
                case when trim(parametro_8_char)='null' then null else trim(parametro_8_char) end cod_gec_parametro_8,
                case when trim(parametro_8_num)='null' then null else trim(parametro_8_num) end cod_gec_parametro_8num,
                case when trim(parametro_9_char)='null' then null else trim(parametro_9_char) end cod_gec_parametro_9,
                case when trim(parametro_9_num)='null' then null else trim(parametro_9_num) end cod_gec_parametro_9num,
                case when trim(parametro_10_char)='null' then null else trim(parametro_10_char) end cod_gec_parametro_10,
                case when trim(parametro_10_num)='null' then null else trim(parametro_10_num) end cod_gec_parametro_10num,
                case when trim(parametro_11_char)='null' then null else trim(parametro_11_char) end cod_gec_parametro_11,
                case when trim(parametro_11_num)='null' then null else trim(parametro_11_num) end cod_gec_parametro_11num,
                case when trim(fecha_carga)='null' then null else trim(fecha_carga) end ts_gec_carga,
                case when trim(ind_carrusel)='1' then 1 else 0 end flag_gec_carrusel,
                case when trim(seccion)='null' then null else trim(seccion) end ds_gec_seccion,
                case when trim(ind_logoff)='1' then 1 else 0 end flag_gec_logoff,
                case when trim(hora_inicio)='null' then null else trim(hora_inicio) end cod_gec_hora_inicio,
                case when trim(hora_fin)='null' then null else trim(hora_fin) end cod_gec_hora_finalizacion,
                case when trim(min_inicio)='null' then null else trim(min_inicio) end cod_gec_min_inicio,
                case when trim(min_fin)='null' then null else trim(min_fin) end cod_gec_min_finalizacion,
                case when trim(audiencias)='null' then null else trim(audiencias) end cod_gec_audiencias,
                A.partition_date
from  bi_corp_staging.rio163_rtd_contenido_canal A
left join bi_corp_common.dim_gec_canales B
on A.id_canal=B.ds_gec_canal_nombre
where A.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_GEC') }}';