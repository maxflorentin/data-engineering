set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=4000000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.exec.dynamic.partition.mode=nonstrict;
set mapred.job.queue.name=root.dataeng;

insert overwrite table bi_corp_common.dim_deli_cp_paquetes_crupier
Select
               trim(cod_paquete) as cod_deli_paquete_crupier
               ,desc_paquete as ds_deli_paquete_crupier
               ,'{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Delivery-Daily') }}' as partition_date
From
        (
        select
              cod_paquete
              ,desc_paquete
              ,max(ultima_modif) ultima_modificacion

        from  bi_corp_staging.rio258_Cp_Paquetes_Crupier
        group by cod_paquete ,desc_paquete
        ) A	;