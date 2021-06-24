set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=4000000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.exec.dynamic.partition.mode=nonstrict;
set mapred.job.queue.name=root.dataeng;

insert overwrite table bi_corp_common.dim_deli_cp_canales_venta
Select
               trim(cod_canal_venta) as cod_deli_canal_venta
               ,desc_canal_venta as ds_deli_canal_venta
               ,'{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Delivery-Daily') }}' as partition_date
From
        (
        select
              cod_canal_venta
              ,desc_canal_venta
              ,max(ultima_modif) ultima_modificacion

        from  bi_corp_staging.rio258_Cp_Canales_venta
        group by cod_canal_venta ,desc_canal_venta
        ) A;