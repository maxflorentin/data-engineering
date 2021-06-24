set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=4000000;
set hive.merge.smallfiles.avgsize=16000000;
set mapred.job.queue.name=root.dataeng;

insert overwrite table bi_corp_common.dim_gec_canales
select
         case when trim(canal)='null' then null else trim(canal) end as canal
        ,case when trim(subcanal)='null' then null else trim(subcanal) end as subcanal
        ,case when trim(nombre)='null' then null else trim(nombre) end as nombre
        ,case when trim(descripcion)='null' then null else trim(descripcion) end as descripcion
        ,case when trim(activo)='S' then 1 else 0 end as activo
        ,case when trim(keystore)='null' then null else trim(keystore) end as keystore
        ,partition_date
from  bi_corp_staging.rio170_gec_canales
where partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_GEC') }}';