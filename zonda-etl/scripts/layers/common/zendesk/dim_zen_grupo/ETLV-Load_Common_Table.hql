SET mapred.job.queue.name=root.dataeng;

INSERT OVERWRITE TABLE bi_corp_common.dim_zen_grupo
select
	cast(trim(id) as bigint) as cod_zen_grupo,
	trim(url) as ds_zen_url,
	trim(name) as ds_zen_grupo,
	trim(description) as ds_zen_descripcion,
	case when trim(default)='True' then 1 else 0 end  as flag_zen_default,
	case when trim(deleted)='True' then 1 else 0 end  as flag_zen_deleted,
	regexp_replace(regexp_replace(created_at,"T"," "),"Z","") as ts_zen_creacion,
	regexp_replace(regexp_replace(updated_at,"T"," "),"Z","") as ts_zen_actualizacion,
	partition_date
from bi_corp_staging.zendesk_gr_santander_ar
where partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='max_partition_zendesk_gr_santander_ar', dag_id='LOAD_CMN_Zendesk') }}'

union all

select
	cast(trim(id) as bigint) as cod_zen_grupo,
	trim(url) as ds_zen_url,
	trim(name) as ds_zen_grupo,
	trim(description) as ds_zen_descripcion,
	case when trim(default)='True' then 1 else 0 end  as flag_zen_default,
	case when trim(deleted)='True' then 1 else 0 end  as flag_zen_deleted,
	regexp_replace(regexp_replace(created_at,"T"," "),"Z","") as ts_zen_creacion,
	regexp_replace(regexp_replace(updated_at,"T"," "),"Z","") as ts_zen_actualizacion,
	partition_date
from bi_corp_staging.zendesk_gr_puc_santander
where partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='max_partition_zendesk_gr_puc_santander', dag_id='LOAD_CMN_Zendesk') }}'

union all

select
	cast(trim(id) as bigint) as cod_zen_grupo,
	trim(url) as ds_zen_url,
	trim(name) as ds_zen_grupo,
	trim(description) as ds_zen_descripcion,
	case when trim(default)='True' then 1 else 0 end  as flag_zen_default,
	case when trim(deleted)='True' then 1 else 0 end  as flag_zen_deleted,
	regexp_replace(regexp_replace(created_at,"T"," "),"Z","") as ts_zen_creacion,
	regexp_replace(regexp_replace(updated_at,"T"," "),"Z","") as ts_zen_actualizacion,
	partition_date
from bi_corp_staging.zendesk_gr_comex_santander
where partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='max_partition_zendesk_gr_comex_santander', dag_id='LOAD_CMN_Zendesk') }}'