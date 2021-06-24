set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=4000000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.exec.dynamic.partition.mode=nonstrict;
set mapred.job.queue.name=root.dataeng;

insert overwrite table bi_corp_common.dim_deli_cp_productos_crupier
Select

              trim(cod_producto) as cod_deli_producto_componente
              ,desc_producto as ds_deli_producto_componente
              ,'{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Delivery-Daily') }}' as partition_date
From
        (
        select
              cod_producto
              ,desc_producto
              ,max(ultima_modif) ultima_modificacion

        from  bi_corp_staging.rio258_Cp_Productos_Crupier
        group by
                cod_producto
                ,desc_producto
        ) A	;