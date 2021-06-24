set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=4000000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.exec.dynamic.partition.mode=nonstrict;
set mapred.job.queue.name=root.dataeng;

INSERT overwrite table bi_corp_common.bt_deli_stock
partition(partition_date)
SELECT
         Croupier_Shipping.ID as cod_deli_courier_shipping
        ,Croupier_Shipping.COURIER_ID as cod_deli_courier
        ,Croupier_Shipping.COURIER_TRACKING_ID as cod_deli_courier_tracking
        ,Croupier_Shipping.CREATED_DATE as ts_deli_creacion
        ,Croupier_Shipping.RECEIVED_USER as ds_deli_usuario_recibido
        ,Croupier_Shipping.SHIPPING_STATUS as ds_deli_estado
        ,case when Croupier_Shipping.CONTRACT_CODE='null' then null else Croupier_Shipping.CONTRACT_CODE end as cod_deli_contrato
        ,case when Croupier_Shipping.CONTRACT_SERVICE_TYPE='null' then null else Croupier_Shipping.CONTRACT_SERVICE_TYPE end as ds_deli_contrato_tipo_servicio
        ,case when Croupier_Shipping.CONTRACT_DESCRIPTION='null' then null else Croupier_Shipping.CONTRACT_DESCRIPTION end as ds_deli_contrato
        ,case when Croupier_Shipping.BRANCH_CODE='null' then null else cast(Croupier_Shipping.BRANCH_CODE as int) end as cod_suc_sucursal
        ,case when Croupier_Shipping.BRANCH_NAME='null' then null else Croupier_Shipping.BRANCH_NAME end as ds_deli_sucursal
        ,case when Croupier_Shipping.BRANCH_ADDRESS='null' then null else Croupier_Shipping.BRANCH_ADDRESS end as ds_deli_direccion_sucursal
        ,case when Croupier_Shipping.BRANCH_CITY='null' then null else Croupier_Shipping.BRANCH_CITY end as ds_deli_ciudad_sucursal
        ,case when Croupier_Shipping.PERSON_DOCUMENT='null' then null else Croupier_Shipping.PERSON_DOCUMENT end as cod_deli_nro_documento
        ,Croupier_Shipping.PERSON_NAME as ds_deli_nombre_apellido
        ,cast(Croupier_Shipping.BRANCH_CODE_RECEIVED as int) as cod_suc_sucursal_recibido
        ,Croupier_Shipping.LAST_UPDATE_DATE as dt_deli_ultima_actualizacion
        ,case when Croupier_Shipping.CONTENT='null' then null else Croupier_Shipping.CONTENT end as ds_deli_contenido
        ,Croupier_Shipping.LAST_UPDATE_USER as cod_deli_usuario_ultima_actualizacion
        ,case when Croupier_Shipping.TAG='null' then null else Croupier_Shipping.TAG end as ds_deli_etiqueta
        ,case when Croupier_Shipping.PERSON_NUP='null' then null else Croupier_Shipping.PERSON_NUP end as cod_per_nup
        ,case when Croupier_Shipping.RECEIVED_DATE='null' then null else Croupier_Shipping.RECEIVED_DATE end as ts_deli_recibido
        ,Croupier_Shipping_log.NEW_SHIPPING_STATUS as ds_deli_estado_nuevo_envio
        ,case when Croupier_Shipping_log.OLD_SHIPPING_STATUS='null' then null else Croupier_Shipping_log.OLD_SHIPPING_STATUS end as ds_deli_estado_anterior_envio
        ,cast(Croupier_Shipping_log.CREATED_DATE as string) as dt_deli_creacion_estado
        ,case when Croupier_Shipping_log.MOTIVE='null' then null else Croupier_Shipping_log.MOTIVE end as ds_deli_motivo_estado
        ,Croupier_Shipping_log.OFFICE_USER as cod_deli_usuario_oficina
        ,cast(Croupier_Shipping_log.DERIVED_BRANCH_CODE as int)  as cod_suc_sucursal_derivada
        ,case when Croupier_Shipping.tracking_shipping_id='null' then null else Croupier_Shipping.tracking_shipping_id end as cod_deli_crupier
        ,'{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Delivery-Daily') }}' as partition_date
    FROM bi_corp_staging.rio258_Croupier_Shipping Croupier_Shipping
        left join bi_corp_staging.rio258_Croupier_Shipping_log Croupier_Shipping_log
    on Croupier_Shipping.id=Croupier_Shipping_log.shipping_id
        and Croupier_Shipping_log.CREATED_DATE = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Delivery-Daily') }}'
    where Croupier_Shipping.last_update_Date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Delivery-Daily') }}'
    group by
         Croupier_Shipping.ID
        ,Croupier_Shipping.COURIER_ID
        ,Croupier_Shipping.COURIER_TRACKING_ID
        ,Croupier_Shipping.CREATED_DATE
        ,Croupier_Shipping.RECEIVED_USER
        ,Croupier_Shipping.SHIPPING_STATUS
        ,case when Croupier_Shipping.CONTRACT_CODE='null' then null else Croupier_Shipping.CONTRACT_CODE end
        ,case when Croupier_Shipping.CONTRACT_SERVICE_TYPE='null' then null else Croupier_Shipping.CONTRACT_SERVICE_TYPE end
        ,case when Croupier_Shipping.CONTRACT_DESCRIPTION='null' then null else Croupier_Shipping.CONTRACT_DESCRIPTION end
        ,case when Croupier_Shipping.BRANCH_CODE='null' then null else cast(Croupier_Shipping.BRANCH_CODE as int) end
        ,case when Croupier_Shipping.BRANCH_NAME='null' then null else Croupier_Shipping.BRANCH_NAME end
        ,case when Croupier_Shipping.BRANCH_ADDRESS='null' then null else Croupier_Shipping.BRANCH_ADDRESS end
        ,case when Croupier_Shipping.BRANCH_CITY='null' then null else Croupier_Shipping.BRANCH_CITY end
        ,Croupier_Shipping.PERSON_DOCUMENT
        ,Croupier_Shipping.PERSON_NAME
        ,cast(Croupier_Shipping.BRANCH_CODE_RECEIVED as int)
        ,Croupier_Shipping.LAST_UPDATE_DATE
        ,case when Croupier_Shipping.CONTENT='null' then null else Croupier_Shipping.CONTENT end
        ,Croupier_Shipping.LAST_UPDATE_USER
        ,case when Croupier_Shipping.TAG='null' then null else Croupier_Shipping.TAG end
        ,case when Croupier_Shipping.PERSON_NUP='null' then null else Croupier_Shipping.PERSON_NUP end
        ,case when Croupier_Shipping.RECEIVED_DATE='null' then null else Croupier_Shipping.RECEIVED_DATE end
        ,Croupier_Shipping_log.NEW_SHIPPING_STATUS
        ,case when Croupier_Shipping_log.OLD_SHIPPING_STATUS='null' then null else Croupier_Shipping_log.OLD_SHIPPING_STATUS end
        ,cast(Croupier_Shipping_log.CREATED_DATE as string)
        ,case when Croupier_Shipping_log.MOTIVE='null' then null else Croupier_Shipping_log.MOTIVE end
        ,Croupier_Shipping_log.OFFICE_USER
        ,cast(Croupier_Shipping_log.DERIVED_BRANCH_CODE as int)
        ,case when Croupier_Shipping.tracking_shipping_id='null' then null else Croupier_Shipping.tracking_shipping_id end
        ,'{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Delivery-Daily') }}'
       ;
