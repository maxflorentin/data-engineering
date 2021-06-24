insert overwrite table bi_corp_common.dim_deli_trackingstatus
Select
                trim(id) as cod_deli_estado
                ,trim(code) as ds_deli_estado
                ,case when reason='null' then null else reason end as ds_deli_motivo
                ,case when secondary_reason='null' then null else secondary_reason end as ds_deli_motivo_secundario
                ,courier_status_code as cod_deli_estado_courier
                ,courier as ds_deli_courier
                ,'{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Delivery-Daily') }}' as partition_date
From
        (
        Select
                id
                ,code
                ,reason
                ,secondary_reason
                ,courier_status_code
                ,courier
                ,row_number() over (partition by id order by partition_date desc) as ultimo_estado
        from bi_corp_staging.rio258_tracking_status
        ) A
where ultimo_estado=1
