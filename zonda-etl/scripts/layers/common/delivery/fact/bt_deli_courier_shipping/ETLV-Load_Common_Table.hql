set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=4000000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.exec.dynamic.partition.mode=nonstrict;
set mapred.job.queue.name=root.dataeng;


insert overwrite table bi_corp_common.bt_deli_courier_shipping
partition(partition_date)
select
        case when trim(id)='null'then null else trim(id) end  as cod_deli_shipping,
		case when trim(shipping_id)='null'then null else trim(shipping_id) end  as cod_deli_crupier,
		case when trim(tracking_id)='null'then null else trim(tracking_id) end  as cod_deli_courier_tracking,
		case when trim(courier)='null'then null else trim(courier) end as ds_deli_courier,
		case when trim(contract)='null'then null else trim(contract) end as cod_deli_contract,
		case when trim(customer_full_name)='null'then null else  trim(customer_full_name) end as ds_deli_nombre_cliente,
		case when trim(customer_document_number)='null'then null else trim(customer_document_number) end cod_deli_documento_cliente,
		case when trim(customer_address_street)='null'then null else trim(customer_address_street) end ds_deli_calle,
		case when trim(customer_address_number)='null'then null else trim(customer_address_number) end cod_deli_altura,
		case when trim(customer_address_floor)='null'then null else trim(customer_address_floor) end cod_deli_piso ,
		case when trim(customer_address_department)='null'then null else trim(customer_address_department) end ds_deli_departamento,
		case when trim(customer_address_city)='null'then null else trim(customer_address_city) end ds_deli_provincia,
		case when trim(customer_address_postal_code)='null'then null else trim(customer_address_postal_code) end cod_deli_postal ,
		case when trim(customer_address_province)='null'then null else trim(customer_address_province) end ds_deli_ciudad ,
		case when trim(branch_code)='null'then null else trim(branch_code) end cod_suc_sucursal,
		case when trim(creation_date)='null'then null else trim(creation_date) end ts_deli_creacion ,
		case when trim(canceled)='null'then null else trim(canceled) end flag_deli_cancelado,
		last_update as partition_date
from bi_corp_staging.rio258_shipping
where last_update='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Delivery-Daily') }}'
group by
        id,
		shipping_id,
		tracking_id,
		courier,
		contract,
		customer_full_name,
		customer_document_number,
		customer_address_street,
		customer_address_number,
		customer_address_floor,
		customer_address_department,
		customer_address_city,
		customer_address_postal_code,
		customer_address_province,
		branch_code,
		creation_date,
		canceled,
		last_update;