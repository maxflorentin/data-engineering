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
        ,Croupier_Shipping.CREATED_DATE as dt_deli_creacion
        ,Croupier_Shipping.RECEIVED_USER as ds_deli_usuario_recibido
        ,Croupier_Shipping.SHIPPING_STATUS as ds_deli_estado
        ,Croupier_Shipping.CONTRACT_CODE as cod_deli_contrato
        ,Croupier_Shipping.CONTRACT_SERVICE_TYPE as ds_deli_contrato_tipo_servicio
        ,Croupier_Shipping.CONTRACT_DESCRIPTION as ds_deli_contrato
        ,Croupier_Shipping.BRANCH_CODE as cod_deli_sucursal
        ,Croupier_Shipping.BRANCH_NAME as ds_deli_sucursal
        ,Croupier_Shipping.BRANCH_ADDRESS as ds_deli_direccion_sucursal
        ,Croupier_Shipping.BRANCH_CITY as ds_deli_ciudad_sucursal
        ,Croupier_Shipping.PERSON_DOCUMENT as cod_deli_nro_documento
        ,Croupier_Shipping.PERSON_NAME as ds_deli_nombre_apellido
        ,Croupier_Shipping.BRANCH_CODE_RECEIVED as cod_deli_sucursal_recibido
        ,Croupier_Shipping.LAST_UPDATE_DATE as ds_deli_ultima_actualizacion
        ,Croupier_Shipping.CONTENT as ds_deli_contenido
        ,Croupier_Shipping.LAST_UPDATE_USER as cod_deli_usuario_ultima_actualizacion
        ,Croupier_Shipping.TAG as ds_deli_etiqueta
        ,Croupier_Shipping.PERSON_NUP as cod_per_nup
        ,Croupier_Shipping.RECEIVED_DATE as dt_deli_recibido
        ,Croupier_Shipping_log.NEW_SHIPPING_STATUS as ds_deli_estado_nuevo_envio
        ,Croupier_Shipping_log.OLD_SHIPPING_STATUS as ds_deli_estado_anterior_envio
        ,Croupier_Shipping_log.CREATED_DATE as dt_deli_creacion_estado
        ,Croupier_Shipping_log.MOTIVE as ds_deli_motivo_estado
        ,Croupier_Shipping_log.OFFICE_USER as cod_deli_usuario_oficina
        ,Croupier_Shipping_log.DERIVED_BRANCH_CODE as cod_deli_sucursal_derivada
        ,'{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Delivery-Daily') }}' as partition_date
    FROM bi_corp_staging.rio258_Croupier_Shipping Croupier_Shipping
        left join bi_corp_staging.rio258_Croupier_Shipping_log Croupier_Shipping_log
    on Croupier_Shipping.id=Croupier_Shipping_log.shipping_id
        and Croupier_Shipping_log.CREATED_DATE = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Delivery-Daily') }}'
    where Croupier_Shipping.last_update_Date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Delivery-Daily') }}'
       ;