set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=4000000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.exec.dynamic.partition.mode=nonstrict;
set mapred.job.queue.name=root.dataeng;

insert overwrite table bi_corp_common.dim_deli_cp_productos_colores
Select
               trim(cod_color) as cod_deli_color_producto_tarjeta
              ,trim(desc_color) as ds_deli_color_producto_tarjeta
              ,'{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Delivery-Daily') }}' as partition_date
From
        (
        select
              cod_color
              ,desc_color
              ,max(ultima_modif) ultima_modificacion

        from  bi_corp_staging.rio258_cp_productos_colores
        group by
                cod_color
                ,desc_color
        ) A	;