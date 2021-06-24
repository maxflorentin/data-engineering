SET mapred.job.queue.name=root.dataeng;
INSERT OVERWRITE TABLE bi_corp_common.dim_zen_form
select
		cast(trim(id) as BIGINT) as cod_zen_form,
		trim(url) as ds_zen_url,
		trim(name) as ds_zen_form,
		case when trim(display_name)='0' then null else trim(display_name) end as ds_zen_display,
		case when trim(end_user_visible)='True' then 1 else 0 end as flag_zen_usuario_visible,
		cast(trim(position) as int) as cod_zen_posicion,
		trim(ticket_field_ids) ds_zen_grupo_field,
		case when trim(active)='True' then 1 else 0 end  as flag_zen_estado,
		case when trim(default)='True' then 1 else 0 end  as flag_zen_default,
		regexp_replace(regexp_replace(created_ats,"T"," "),"Z","") as ts_zen_creacion,
		regexp_replace(regexp_replace(updated_ats,"T"," "),"Z","") as ts_zen_actualizacion,
		case when trim(in_all_brands)='True' then 1 else 0 end as flag_zen_todos_los_brands,
		trim(restricted_brand_ids) ds_zen_brand_restricted,
		trim(end_user_conditions) as ds_zen_condiciones_usuario_final,
		partition_date
from bi_corp_staging.zendesk_tk_forms_santander_ar
where partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='max_partition_zendesk_tk_forms_santander_ar', dag_id='LOAD_CMN_Zendesk_Hist2') }}'

union all

select
		cast(trim(id) as BIGINT) as cod_zen_form,
		trim(url) as ds_zen_url,
		trim(name) as ds_zen_form,
		case when trim(display_name)='0' then null else trim(display_name) end as ds_zen_display,
		case when trim(end_user_visible)='True' then 1 else 0 end as flag_zen_usuario_visible,
		cast(trim(position) as int) as cod_zen_posicion,
		trim(ticket_field_ids) ds_zen_grupo_field,
		case when trim(active)='True' then 1 else 0 end  as flag_zen_estado,
		case when trim(default)='True' then 1 else 0 end  as flag_zen_default,
		regexp_replace(regexp_replace(created_ats,"T"," "),"Z","") as ts_zen_creacion,
		regexp_replace(regexp_replace(updated_ats,"T"," "),"Z","") as ts_zen_actualizacion,
		case when trim(in_all_brands)='True' then 1 else 0 end as flag_zen_todos_los_brands,
		trim(restricted_brand_ids) ds_zen_brand_restricted,
		trim(end_user_conditions) as ds_zen_condiciones_usuario_final,
		partition_date
from bi_corp_staging.zendesk_tk_forms_puc_santander
where partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='max_partition_zendesk_tk_forms_puc_santander', dag_id='LOAD_CMN_Zendesk_Hist2') }}'

union all

select
		cast(trim(id) as BIGINT) as cod_zen_form,
		trim(url) as ds_zen_url,
		trim(name) as ds_zen_form,
		case when trim(display_name)='0' then null else trim(display_name) end as ds_zen_display,
		case when trim(end_user_visible)='True' then 1 else 0 end as flag_zen_usuario_visible,
		cast(trim(position) as INT) as cod_zen_posicion,
		trim(ticket_field_ids) ds_zen_grupo_field,
		case when trim(active)='True' then 1 else 0 end  as flag_zen_estado,
		case when trim(default)='True' then 1 else 0 end  as flag_zen_default,
		regexp_replace(regexp_replace(created_ats,"T"," "),"Z","") as ts_zen_creacion,
		regexp_replace(regexp_replace(updated_ats,"T"," "),"Z","") as ts_zen_actualizacion,
		case when trim(in_all_brands)='True' then 1 else 0 end as flag_zen_todos_los_brands,
		trim(restricted_brand_ids) ds_zen_brand_restricted,
		trim(end_user_conditions) as ds_zen_condiciones_usuario_final,
		partition_date
from bi_corp_staging.zendesk_tk_forms_comex_santander
where partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='max_partition_zendesk_tk_forms_comex_santander', dag_id='LOAD_CMN_Zendesk_Hist2') }}'

