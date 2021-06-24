set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=4000000;
set hive.merge.smallfiles.avgsize=16000000;
set mapred.job.queue.name=root.dataeng;

insert overwrite table bi_corp_common.dim_gec_mensajes
select
         case when trim(id_mensaje)='null' then null else trim(id_mensaje) end cod_gec_notificacion_interno
        ,case when trim(codigo)='null' then null else trim(codigo) end cod_gec_notificacion
        ,case when trim(sub_codigo)='null' then null else trim(sub_codigo) end cod_gec_subnotificacion
        ,case when trim(mensaje)='null' then null else trim(mensaje) end ds_gec_notificacion
        ,case when trim(titulo)='null' then null else trim(titulo) end ds_gec_titulo
        ,case when trim(url_desktop)='null' then null else trim(url_desktop) end ds_gec_url_desktop
        ,case when trim(url_mobile)='null' then null else trim(url_mobile) end ds_gec_url_mobile
        ,case when trim(link)='null' then null else trim(link) end  ds_gec_link
        ,partition_date
from  bi_corp_staging.rio170_gec_mensajes
where partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_GEC') }}';
