SET mapred.job.queue.name=root.dataeng;

INSERT OVERWRITE TABLE bi_corp_common.dim_zen_field
select
		cast(trim(id) as bigint) as cod_zen_field,
		trim(url) as ds_zen_url,
		trim(title) as ds_zen_titulo,
		case when trim(description)='0' then null else trim(description) end as ds_zen_descripcion_detalle,
		case when trim(position)='9999' then null else trim(position) end  as cod_zen_posicion,
		case when trim(active)='True' then 1 else 0 end as flag_zen_estado,
		case when trim(required)='True' then 1 else 0 end as flag_zen_requerido,
		case when trim(collapsed_for_agents)='True' then 1 else 0 end as flag_zen_collapsed_for_agents,
		case when trim(regexp_for_validation)='None' then null else trim(regexp_for_validation) end as cod_zen_regexp_for_validation,
		trim(title_in_portal) as ds_zen_titulo_portal,
		case when trim(visible_in_portal)='True' then 1 else 0 end as flag_zen_visible_en_portal,
		case when trim(editable_in_portal)='True' then 1 else 0 end as flag_zen_editable_en_portal,
		case when trim(required_in_portal)='True' then 1 else 0 end as flag_zen_requerido_en_portal,
		case when trim(tag)='None' then null else trim(tag) end as ds_zen_etiqueta,
		regexp_replace(regexp_replace(created_at,"T"," "),"Z","") as ts_zen_creacion,
		regexp_replace(regexp_replace(updated_at,"T"," "),"Z","") as ts_zen_actualizacion,
		case when trim(removable)='True' then 1 else 0 end as flag_zen_removable,
		case when trim(agent_description)='None' then null else trim(agent_description) end as ds_zen_agente,
		partition_date
from bi_corp_staging.zendesk_tk_fields_santander_ar
where partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='max_partition_zendesk_tk_fields_santander_ar', dag_id='LOAD_CMN_Zendesk_Hist2') }}'

union all

select
		cast(trim(id) as bigint) as cod_zen_field,
		trim(url) as ds_zen_url,
		trim(title) as ds_zen_titulo,
		case when trim(description)='0' then null else trim(description) end as ds_zen_descripcion_detalle,
		case when trim(position)='9999' then null else trim(position) end  as cod_zen_posicion,
		case when trim(active)='True' then 1 else 0 end as flag_zen_estado,
		case when trim(required)='True' then 1 else 0 end as flag_zen_requerido,
		case when trim(collapsed_for_agents)='True' then 1 else 0 end as flag_zen_collapsed_for_agents,
		case when trim(regexp_for_validation)='None' then null else trim(regexp_for_validation) end as cod_zen_regexp_for_validation,
		trim(title_in_portal) as ds_zen_titulo_portal,
		case when trim(visible_in_portal)='True' then 1 else 0 end as flag_zen_visible_en_portal,
		case when trim(editable_in_portal)='True' then 1 else 0 end as flag_zen_editable_en_portal,
		case when trim(required_in_portal)='True' then 1 else 0 end as flag_zen_requerido_en_portal,
		case when trim(tag)='None' then null else trim(tag) end as ds_zen_etiqueta,
		regexp_replace(regexp_replace(created_at,"T"," "),"Z","") as ts_zen_creacion,
		regexp_replace(regexp_replace(updated_at,"T"," "),"Z","") as ts_zen_actualizacion,
		case when trim(removable)='True' then 1 else 0 end as flag_zen_removable,
		case when trim(agent_description)='None' then null else trim(agent_description) end as ds_zen_agente,
		partition_date
from bi_corp_staging.zendesk_tk_fields_puc_santander
where partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='max_partition_zendesk_tk_fields_puc_santander', dag_id='LOAD_CMN_Zendesk_Hist2') }}'


union all


select
		cast(trim(id) as bigint) as cod_zen_field,
		trim(url) as ds_zen_url,
		trim(title) as ds_zen_titulo,
		case when trim(description)='0' then null else trim(description) end as ds_zen_descripcion_detalle,
		case when trim(position)='9999' then null else trim(position) end  as cod_zen_posicion,
		case when trim(active)='True' then 1 else 0 end as flag_zen_estado,
		case when trim(required)='True' then 1 else 0 end as flag_zen_requerido,
		case when trim(collapsed_for_agents)='True' then 1 else 0 end as flag_zen_collapsed_for_agents,
		case when trim(regexp_for_validation)='None' then null else trim(regexp_for_validation) end as cod_zen_regexp_for_validation,
		trim(title_in_portal) as ds_zen_titulo_portal,
		case when trim(visible_in_portal)='True' then 1 else 0 end as flag_zen_visible_en_portal,
		case when trim(editable_in_portal)='True' then 1 else 0 end as flag_zen_editable_en_portal,
		case when trim(required_in_portal)='True' then 1 else 0 end as flag_zen_requerido_en_portal,
		case when trim(tag)='None' then null else trim(tag) end as ds_zen_etiqueta,
		regexp_replace(regexp_replace(created_at,"T"," "),"Z","") as ts_zen_creacion,
		regexp_replace(regexp_replace(updated_at,"T"," "),"Z","") as ts_zen_actualizacion,
		case when trim(removable)='True' then 1 else 0 end as flag_zen_removable,
		case when trim(agent_description)='None' then null else trim(agent_description) end as ds_zen_agente,
		partition_date
from bi_corp_staging.zendesk_tk_fields_comex_santander
where partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='max_partition_zendesk_tk_fields_comex_santander', dag_id='LOAD_CMN_Zendesk_Hist2') }}'