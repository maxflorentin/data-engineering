set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=4000000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.exec.dynamic.partition.mode=nonstrict;
set mapred.job.queue.name=root.dataeng;

insert overwrite table bi_corp_common.bt_gec_notificacion_canal_cliente
partition(partition_date)
SELECT
        Distinct -- Se coloca distinct porque puede presentarse dos eventos iguales pero con diferencia de decimas de segundos
         case when trim(id_notificacion)='null' then null else trim(id_notificacion) end cod_gec_notificacion_interno
        ,case when trim(cod_canal)='null' then null else trim(cod_canal) end cod_gec_canal
        ,case when trim(cod_subcanal)='null' then null else trim(cod_subcanal) end cod_gec_subcanal
        ,B.ds_gec_canal
        ,case when trim(nup)='null' then null else trim(nup) end cod_per_nup
        ,case when trim(accion)='null' then null else trim(accion) end ds_gec_accion
        ,case when trim(fecha_estado)='null' then null else trim(fecha_estado) end ts_gec_estado
        ,case when trim(comentarios)='null' then null else trim(comentarios) end ds_comentarios
        ,case when trim(codigo)='null' then null else trim(codigo) end cod_gec_notificacion
        ,case when trim(sub_codigo)='null' then null else trim(sub_codigo) end cod_gec_subnotificacion
        ,case when trim(id_grupo)='null' then null else trim(id_grupo) end cod_gec_grupo
        ,A.partition_date
FROM  bi_corp_staging.rio170_gec_notif_canal_cliente A
left join bi_corp_common.dim_gec_canales B
on A.cod_canal=B.cod_gec_canal
and A.cod_subcanal=B.cod_gec_subcanal
WHERE A.partition_date= '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_GEC') }}';