set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=4000000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.exec.dynamic.partition.mode=nonstrict;
set mapred.job.queue.name=root.dataeng;

insert overwrite table bi_corp_common.bt_gec_ofertas_presentadas
partition(partition_date)
SELECT
		    case when trim(id_oferta_rtd)='null' then null else trim(id_oferta_rtd) end cod_gec_oferta,
            case when trim(id_oferta_interno)='null' then null else trim(id_oferta_interno) end cod_gec_oferta_interno ,
            case when trim(id_canal)='null' then null else trim(id_canal) end cod_gec_canal ,
            B.ds_gec_canal,
            case when trim(nup)='null' then null else trim(nup) end cod_per_nup,
            case when trim(accion)='null' then null else trim(accion) end ds_gec_accion ,
            case when trim(fecha)='null' then null else trim(fecha) end ts_gec_presentacion ,
            case when trim(tipo_oferta)='null' then null else trim(tipo_oferta) end ds_gec_tipo_oferta ,
            case when trim(categoria_oferta)='null' then null else trim(categoria_oferta) end ds_gec_categoria_oferta ,
            case when trim(grupo_control)='null' then null else trim(grupo_control) end cod_gec_grupo_control,
            case when trim(fecha_carga)='null' then null else trim(fecha_carga) end ts_gec_carga ,
            case when trim(probabilidad)='null' then null else trim(probabilidad) end cod_gec_probabilidad ,
            case when trim(ubicacion_carrusel)='S' then 1 else 0 end flag_gec_carrusel ,
            case when trim(ubicacion_seccion)='null' then null else trim(ubicacion_seccion) end ds_gec_ubicacion_seccion,
            case when trim(id_rtd)='null' then null else trim(id_rtd) end cod_gec_rtd,
            case when trim(fecha_ult_actualizacion)='null' then null else trim(fecha_ult_actualizacion) end ts_gec_ult_actualizacion ,
            case when trim(orden_prioridad)='null' then null else trim(orden_prioridad) end cod_gec_orden_prioridad ,
            case when trim(tipo_ofrecimiento)='null' then null else trim(tipo_ofrecimiento) end cod_gec_tipo_ofrecimiento ,
            case when trim(origen)='null' then null else trim(origen) end ds_gec_origen,
		    A.partition_date
FROM  bi_corp_staging.rio163_rtd_ofertas_presentadas A
left join bi_corp_common.dim_gec_canales B
on A.id_canal=B.ds_gec_canal_nombre
WHERE A.partition_date= '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_GEC') }}';