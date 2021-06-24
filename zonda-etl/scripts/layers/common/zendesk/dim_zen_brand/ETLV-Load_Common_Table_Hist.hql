SET mapred.job.queue.name=root.dataeng;

INSERT OVERWRITE TABLE bi_corp_common.dim_zen_brand
select
		cast(trim(id) as bigint) as cod_zen_brand,
		trim(url) as ds_zen_url,
		trim(name) as ds_zen_brand,
		trim(brand_url) as ds_zen_url_brand,
		trim(subdomain) as ds_zen_subdominio,
		case when trim(has_help_center)='True' then 1 else 0 end as flag_zen_help_center,
		trim(help_center_state) as ds_estado_help_center,
		case when trim(active)='True' then 1 else 0 end  as flag_zen_estado,
		case when trim(default)='True' then 1 else 0 end  as flag_zen_default,
		case when trim(is_deleted)='True' then 1 else 0 end  as flag_zen_deleted,
		case when trim(logo)='None' then Null else trim(logo) end as ds_zen_logo,
		trim(ticket_form_ids) ds_zen_form,
		trim(signature_template) ds_zen_signature_template,
		regexp_replace(regexp_replace(created_ats,"T"," "),"Z","") as ts_zen_creacion,
		regexp_replace(regexp_replace(updated_ats,"T"," "),"Z","") as ts_zen_actualizacion,
		case when trim(host_mappings)='None' then Null else trim(host_mappings) end as ds_zen_host_mapping,
		'{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Zendesk_Hist2') }}' as partition_date
from bi_corp_staging.zendesk_brands_santander_ar
where partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='max_partition_zendesk_brands_santander_ar', dag_id='LOAD_CMN_Zendesk_Hist2') }}'

UNION ALL

select
		cast(trim(id) as bigint) as cod_zen_brand,
		trim(url) as ds_zen_url,
		trim(name) as ds_zen_brand,
		trim(brand_url) as ds_zen_url_brand,
		trim(subdomain) as ds_zen_subdominio,
		case when trim(has_help_center)='True' then 1 else 0 end as flag_zen_help_center,
		trim(help_center_state) as ds_estado_help_center,
		case when trim(active)='True' then 1 else 0 end  as flag_zen_estado,
		case when trim(default)='True' then 1 else 0 end  as flag_zen_default,
		case when trim(is_deleted)='True' then 1 else 0 end  as flag_zen_deleted,
		case when trim(logo)='None' then Null else trim(logo) end as ds_zen_logo,
		trim(ticket_form_ids) ds_zen_form,
		trim(signature_template) ds_zen_signature_template,
		regexp_replace(regexp_replace(created_ats,"T"," "),"Z","") as ts_zen_creacion,
		regexp_replace(regexp_replace(updated_ats,"T"," "),"Z","") as ts_zen_actualizacion,
		case when trim(host_mappings)='None' then Null else trim(host_mappings) end as ds_zen_host_mapping,
		'{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Zendesk_Hist2') }}' as partition_date
from bi_corp_staging.zendesk_brands_comex_santander
where partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='max_partition_zendesk_brands_comex_santander', dag_id='LOAD_CMN_Zendesk_Hist2') }}'

UNION ALL


select
		cast(trim(id) as bigint) as cod_zen_brand,
		trim(url) as ds_zen_url,
		trim(name) as ds_zen_brand,
		trim(brand_url) as ds_zen_url_brand,
		trim(subdomain) as ds_zen_subdominio,
		case when trim(has_help_center)='True' then 1 else 0 end as flag_zen_help_center,
		trim(help_center_state) as ds_estado_help_center,
		case when trim(active)='True' then 1 else 0 end  as flag_zen_estado,
		case when trim(default)='True' then 1 else 0 end  as flag_zen_default,
		case when trim(is_deleted)='True' then 1 else 0 end  as flag_zen_deleted,
		case when trim(logo)='None' then Null else trim(logo) end as ds_zen_logo,
		trim(ticket_form_ids) ds_zen_form,
		trim(signature_template) ds_zen_signature_template,
		regexp_replace(regexp_replace(created_ats,"T"," "),"Z","") as ts_zen_creacion,
		regexp_replace(regexp_replace(updated_ats,"T"," "),"Z","") as ts_zen_actualizacion,
		case when trim(host_mappings)='None' then Null else trim(host_mappings) end as ds_zen_host_mapping,
		'{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Zendesk_Hist2') }}' as partition_date
from bi_corp_staging.zendesk_brands_puc_santander
where partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='max_partition_zendesk_brands_puc_santander', dag_id='LOAD_CMN_Zendesk_Hist2') }}'