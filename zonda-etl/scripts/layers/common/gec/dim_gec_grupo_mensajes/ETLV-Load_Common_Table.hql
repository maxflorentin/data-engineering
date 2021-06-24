set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=4000000;
set hive.merge.smallfiles.avgsize=16000000;
set mapred.job.queue.name=root.dataeng;

insert overwrite table bi_corp_common.dim_gec_grupo_mensajes
select
         id_grupo
        ,id_mensaje
        ,cod_canal
        ,cod_subcanal
        ,B.ds_gec_canal
        ,A.partition_date
from  bi_corp_staging.rio170_gec_grupo_mensajes A
left join bi_corp_common.dim_gec_canales B
on A.cod_canal=B.cod_gec_canal
and A.cod_subcanal=B.cod_gec_subcanal
where A.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_GEC') }}';