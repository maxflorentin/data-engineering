set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=4000000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.exec.dynamic.partition.mode=nonstrict;
set mapred.job.queue.name=root.dataeng;

insert overwrite table bi_corp_common.dim_deli_cp_productos_tarjetas
Select
				 trim(cod_prod_tarjeta) as cod_deli_prod_tarjeta
                ,trim(cod_prod) as cod_deli_subproducto_tarjeta
                ,desc_prod as ds_deli_producto_tarjeta
                ,trim(cod_origen) as cod_deli_origen_producto_tarjeta
                ,trim(cod_color) as cod_deli_color_producto_tarjeta
                ,'{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Delivery-Daily') }}' as partition_date
From
        (
        select
				cod_prod_tarjeta
                ,cod_prod
                ,desc_prod
                ,cod_origen
                ,cod_color
                ,max(ultima_modif) ultima_modificacion

        from  bi_corp_staging.rio258_Cp_Productos_Tarjetas
        group by
				cod_prod_tarjeta
                ,cod_prod
                ,desc_prod
                ,cod_origen
                ,cod_color
        ) A		;