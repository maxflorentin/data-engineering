set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=4000000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.exec.dynamic.partition.mode=nonstrict;
set mapred.job.queue.name=root.dataeng;

insert overwrite table bi_corp_common.bt_deli_envios_estados
partition(partition_date)
SELECT
         trim(cpee.COD_ENVIO_ESTADO) as   cod_deli_estado_envio
        ,trim(cpee.COD_ESTADO)    as cod_deli_estado
        ,trim(cpee.FECHA_ESTADO) as ts_deli_estado
        ,trim(cpee.ULTIMA_MODIF) as dt_deli_ultima_modificacion_estado
        ,date_format(trim(cpee.FECHA_ESTADO_SOLR),'yyyy-MM-dd') as dt_deli_estado_solr
        ,trim(cpee.CREADO_POR) as ds_deli_creador_estado
        ,trim(cpee.ULTIMA_MODIF_POR) as ds_deli_ultimo_modificador_estado
        ,trim(crupier_id) AS cod_deli_crupier
        ,Cp_Estados_Crupier_env.ds_deli_estado_crupier_env
        ,'{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Delivery-Daily') }}' as partition_date
FROM bi_corp_staging.rio258_Cp_Envios_estados cpee
left join bi_corp_common.dim_deli_cp_estados_crupier_env Cp_Estados_Crupier_env
on trim(cpee.Cod_Estado) = cast(Cp_Estados_Crupier_env.cod_deli_estado_crupier_env as string)
where cpee.ultima_modif = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Delivery-Daily') }}'
;