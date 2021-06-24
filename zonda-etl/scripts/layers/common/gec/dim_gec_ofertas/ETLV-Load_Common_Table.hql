set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=4000000;
set hive.merge.smallfiles.avgsize=16000000;
set mapred.job.queue.name=root.dataeng;

insert overwrite table bi_corp_common.dim_gec_ofertas
select
                    case when trim(id_oferta_rtd)='null' then null else trim(id_oferta_rtd) end cod_gec_oferta,
                    case when trim(id_oferta_interno)='null' then null else trim(id_oferta_interno) end cod_gec_oferta_interno,
                    case when trim(categoria_oferta)='null' then null else trim(categoria_oferta) end ds_gec_categoria_oferta,
                    case when trim(tipo_oferta)='null' then null else trim(tipo_oferta) end ds_gec_tipo_oferta,
                    case when trim(segmento_aplica)='null' then null else trim(segmento_aplica) end ds_gec_segmento_aplica,
                    case when trim(nombre_oferta)='null' then null else trim(nombre_oferta) end ds_gec_oferta,
                    case when trim(fecha_inicio)='null' then null else trim(fecha_inicio) end ts_gec_inicio,
                    case when trim(fecha_finalizacion)='null' then null else trim(fecha_finalizacion) end ts_gec_finalizacion,
                    case when trim(pg_1_score)='null' then null else trim(pg_1_score) end cod_gec_pg1_score,
                    case when trim(pg_2_score)='null' then null else trim(pg_2_score) end cod_gec_pg2_score,
                    case when trim(pg_3_score)='null' then null else trim(pg_3_score) end cod_gec_pg3_score,
                    case when trim(pg_4_score)='null' then null else trim(pg_4_score) end cod_gec_pg4_score,
                    case when trim(localidad)='null' then null else trim(localidad) end ds_gec_localidad,
                    case when trim(provincia)='null' then null else trim(provincia) end ds_gec_provincia,
                    case when trim(cant_puntos_base)='null' then null else trim(cant_puntos_base) end fc_gec_cant_puntos_base ,
                    case when trim(url)='null' then null else trim(url) end ds_gec_url,
                    case when trim(dia_sem_aplica_beneficio)='null' then null else trim(dia_sem_aplica_beneficio) end ds_gec_dia_aplica,
                    case when trim(fecha_carga)='null' then null else trim(fecha_carga) end ts_gec_carga,
                    case when trim(seccion)='null' then null else trim(seccion) end cod_gec_seccion,
                    case when trim(id_loyalty)='null' then null else trim(id_loyalty) end cod_gec_loyalty,
                    case when trim(producto)='null' then null else trim(producto) end cod_gec_producto,
                    case when trim(familia)='null' then null else trim(familia) end ds_gec_familia,
                    case when trim(valida_producto_familia)='null' then null else trim(valida_producto_familia) end cod_gec_valida_producto_familia,
                    partition_date
from  bi_corp_staging.rio163_rtd_ofertas
where partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_GEC') }}';