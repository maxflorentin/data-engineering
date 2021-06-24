set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=4000000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.exec.dynamic.partition.mode=nonstrict;
set mapred.job.queue.name=root.dataeng;

insert overwrite table bi_corp_common.bt_deli_componentes_estados
partition(partition_date)
SELECT
         trim(Cp_Envios_Comp_Est.COD_ENVIO_COMP_ESTADO) as cod_deli_componente_envio
        ,trim(Cp_Envios_Comp_Est.COD_ESTADO) as cod_deli_estado
        ,date_format(Cp_Envios_Comp_Est.FECHA_NOVEDAD,'yyyy-MM-dd') as dt_deli_novedad_estado
        ,Cp_Envios_Comp_Est.ULTIMA_MODIF as dt_deli_ultima_modificacion_estado
        ,Cp_Envios_Comp_Est.CREADO_POR as ds_deli_creador_estado
        ,Cp_Envios_Comp_Est.ULTIMA_MODIF_POR as ds_deli_ultimo_modificador_estado
        ,case when Cp_Envios_Comp_Est.COD_ERROR='null' then null else Cp_Envios_Comp_Est.COD_ERROR end as cod_deli_error
        ,case when Cp_Envios_Comp_Est.DESC_ERROR='null' then null else Cp_Envios_Comp_Est.DESC_ERROR end as ds_deli_error
        ,trim(cod_componente) AS cod_deli_componente
        ,Cp_Est_Crup_comp.ds_deli_estado_crupier_comp
        ,'{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Delivery-Daily') }}'  as partition_date
FROM  bi_corp_staging.rio258_Cp_Envios_Componentes_Estados Cp_Envios_Comp_Est
left join bi_corp_common.dim_deli_cp_estados_crupier_comp Cp_Est_Crup_comp
on trim(Cp_Envios_Comp_Est.COD_ESTADO)=cast(Cp_Est_Crup_comp.cod_deli_estado_crupier_comp as string)
where Cp_Envios_Comp_Est.ultima_modif= '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Delivery-Daily') }}';

