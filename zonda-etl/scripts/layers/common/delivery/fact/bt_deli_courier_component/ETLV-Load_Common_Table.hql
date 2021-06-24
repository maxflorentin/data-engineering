set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=4000000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.exec.dynamic.partition.mode=nonstrict;
set mapred.job.queue.name=root.dataeng;


insert overwrite table bi_corp_common.bt_deli_courier_component
partition(partition_date)
select
		case when trim(internal_shipping_id)='null' then null else trim(internal_shipping_id) end cod_deli_shipping,
		case when trim(code)='null' then null else trim(code) end  as cod_deli_codigo,
		case when trim(type)='null' then null else trim(type) end as ds_deli_tipo,
		CASE WHEN trim(TRACKING_ID)='null' THEN NULL ELSE trim(TRACKING_ID) end as cod_deli_componente_tracking,
		case when trim(event_date)='null' then null else trim(event_date) end  as ts_deli_evento,
		partition_date
from bi_corp_staging.rio258_shipping_component
where partition_date ='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Delivery-Daily') }}'
group by
		internal_shipping_id,
		code,
		type,
		TRACKING_ID,
		event_date,
		partition_date;