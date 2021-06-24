set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=4000000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.exec.dynamic.partition.mode=nonstrict;
set mapred.job.queue.name=root.dataeng;

insert overwrite table bi_corp_common.dim_deli_cp_estados_crupier_env
Select
               trim(cod_estado) as cod_deli_estado_crupier_env
               ,desc_estado as ds_deli_estado_crupier_env
               ,'{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Delivery-Daily') }}' as partition_date
From
        (
        select
              cod_estado
              ,desc_estado
              ,max(ultima_modif) ultima_modificacion

        from  bi_corp_staging.rio258_Cp_Estados_Crupier_env
        group by cod_estado ,desc_estado
        ) A
        	;