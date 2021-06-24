set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=4000000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.exec.dynamic.partition.mode=nonstrict;
set mapred.job.queue.name=root.dataeng;


insert overwrite table bi_corp_common.bt_deli_courier_event
partition(partition_date)
select
        case when trim(shipping_event.id)='null' then null else trim(shipping_event.id) end as cod_deli_courier_event,
		case when trim(shipping_event.internal_shipping_id)='null' then null else trim(shipping_event.internal_shipping_id) end as cod_deli_shipping,
		case when trim(shipping_event.event_date)='null' then null else trim(shipping_event.event_date) end as ts_deli_evento,
		case when trim(shipping_event.branch_code)='null' then null else trim(shipping_event.branch_code) end as cod_suc_sucursal,
		case when trim(shipping_event.courier_branch_code)='null' then null else trim(shipping_event.courier_branch_code) end as cod_suc_sucursal_courier,
		case when trim(shipping_event.uuid)='null' then null else trim(shipping_event.uuid)end as cod_deli_uuid,
		case when trim(shipping_event.processed_date)='null' then null else trim(shipping_event.processed_date) end as ts_deli_proceso,
		case when trim(shipping_event.status_id)='null' then null else trim(shipping_event.status_id) end  as cod_deli_estado,
	    tracking_status.ds_deli_estado,
        tracking_status.ds_deli_motivo,
        tracking_status.ds_deli_motivo_secundario,
        tracking_status.cod_deli_estado_courier,
        shipping_event.partition_date
from bi_corp_staging.rio258_shipping_event shipping_event
left join bi_corp_common.dim_deli_trackingstatus tracking_status
on trim(shipping_event.STATUS_ID) = cast(tracking_status.cod_deli_estado as string)
where shipping_event.partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Delivery-Daily') }}'
group by
        case when trim(shipping_event.id)='null' then null else trim(shipping_event.id) end,
		case when trim(shipping_event.internal_shipping_id)='null' then null else trim(shipping_event.internal_shipping_id) end,
		case when trim(shipping_event.event_date)='null' then null else trim(shipping_event.event_date) end ,
		case when trim(shipping_event.branch_code)='null' then null else trim(shipping_event.branch_code) end ,
		case when trim(shipping_event.courier_branch_code)='null' then null else trim(shipping_event.courier_branch_code) end ,
		case when trim(shipping_event.uuid)='null' then null else trim(shipping_event.uuid)end ,
		case when trim(shipping_event.processed_date)='null' then null else trim(shipping_event.processed_date) end ,
		case when trim(shipping_event.status_id)='null' then null else trim(shipping_event.status_id) end  ,
	    tracking_status.ds_deli_estado,
        tracking_status.ds_deli_motivo,
        tracking_status.ds_deli_motivo_secundario,
        tracking_status.cod_deli_estado_courier,
        shipping_event.partition_date
;