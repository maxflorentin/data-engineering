SET mapred.job.queue.name=root.dataeng;
set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=4000000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.exec.dynamic.partition.mode=nonstrict;


insert overwrite table bi_corp_common.bt_zen_ticket
partition(partition_date)
SELECT
	trim(url) ds_zen_url,
	trim(id) cod_zen_ticket,
	case when trim(external_id)='None' then null else trim(external_id) end as cod_zen_external,
	CASE WHEN regexp_replace(split(via,"\'")[3],"_==channel==","")='any' Then lower(regexp_replace(split(via,"\'")[25],"_==channel==",""))
		 ELSE regexp_replace(split(via,"\'")[3],"_==channel==","") END AS ds_zen_canal,
	regexp_replace(regexp_replace(created_at,"T"," "),"Z","") as ts_zen_creacion,
	regexp_replace(regexp_replace(updated_at,"T"," "),"Z","") as ts_zen_actualizacion,
	trim(subject) as ds_zen_asunto,
	trim(description) as ds_zen_ticket,
	case when trim(priority)='None' then null else trim(priority) end as ds_zen_prioridad,
	trim(status) as ds_zen_status,
	case when trim(recipient)='None' then null else trim(recipient) end as cod_zen_destinatario,
	trim(requester_id) as cod_zen_usuario_solicitante,
	solicitante.ds_zen_nombre as ds_zen_usuario_solicitante,
	trim(submitter_id) as cod_zen_usuario_remitente,
	remitente.ds_zen_nombre as ds_zen_usuario_remitente,
	trim(assignee_id) as cod_zen_usuario_asignado,
	asignado.ds_zen_nombre as ds_zen_usuario_asignado,
	case when trim(organization_id)='None' then null else trim(organization_id) end as cod_zen_organizacion,
	case when trim(group_id)='None' then null else trim(group_id) end as cod_zen_grupo,
	grupo.ds_zen_grupo,
	trim(collaborator_ids) cod_zen_usuarios_colaboradores,
	trim(email_cc_ids) cod_zen_email_cc,
	case when trim(is_public)='True' then 1 else 0 end as flag_zen_publico,
	trim(tags) as ds_zen_etiquetas,
	trim(custom_fields) as ds_zen_campos_personalizados,
	get_json_object(regexp_replace(satisfaction_rating ,"\'","\"") ,'$.score') ds_zen_satisfaccion,
	trim(ticket_form_id) as cod_zen_form ,
	form.ds_zen_form,
	trim(brand_id) as cod_zen_brand,
	brand.ds_zen_brand,
	ticket.partition_date
from bi_corp_staging.zendesk_tk_santander_ar ticket
left join bi_corp_common.stk_zen_usuarios solicitante
on trim(ticket.requester_id)=solicitante.cod_zen_usuario
AND solicitante.partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Zendesk_Hist3') }}'
left join bi_corp_common.stk_zen_usuarios remitente
on trim(ticket.submitter_id)=remitente.cod_zen_usuario
AND remitente.partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Zendesk_Hist3') }}'
left join bi_corp_common.stk_zen_usuarios asignado
on trim(ticket.assignee_id)= asignado.cod_zen_usuario
AND asignado.partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Zendesk_Hist3') }}'
left join bi_corp_common.dim_zen_grupo grupo
on trim(group_id)=grupo.cod_zen_grupo
left join bi_corp_common.dim_zen_form form
on ticket.ticket_form_id=form.cod_zen_form
left join bi_corp_common.dim_zen_brand brand
on ticket.brand_id=brand.cod_zen_brand
WHERE ticket.partition_date= '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Zendesk_Hist3') }}'

UNION ALL

SELECT
	trim(url) ds_zen_url,
	trim(id) cod_zen_ticket,
	case when trim(external_id)='None' then null else trim(external_id) end as cod_zen_external,
	CASE WHEN regexp_replace(split(via,"\'")[3],"_==channel==","")='any' Then lower(regexp_replace(split(via,"\'")[25],"_==channel==",""))
		 ELSE regexp_replace(split(via,"\'")[3],"_==channel==","") END AS ds_zen_canal,
	regexp_replace(regexp_replace(created_at,"T"," "),"Z","") as ts_zen_creacion,
	regexp_replace(regexp_replace(updated_at,"T"," "),"Z","") as ts_zen_actualizacion,
	trim(subject) as ds_zen_asunto,
	trim(description) as ds_zen_ticket,
	case when trim(priority)='None' then null else trim(priority) end as ds_zen_prioridad,
	trim(status) as ds_zen_status,
	case when trim(recipient)='None' then null else trim(recipient) end as cod_zen_destinatario,
	trim(requester_id) as cod_zen_usuario_solicitante,
	solicitante.ds_zen_nombre as ds_zen_usuario_solicitante,
	trim(submitter_id) as cod_zen_usuario_remitente,
	remitente.ds_zen_nombre as ds_zen_usuario_remitente,
	trim(assignee_id) as cod_zen_usuario_asignado,
	asignado.ds_zen_nombre as ds_zen_usuario_asignado,
	case when trim(organization_id)='None' then null else trim(organization_id) end as cod_zen_organizacion,
	case when trim(group_id)='None' then null else trim(group_id) end as cod_zen_grupo,
	grupo.ds_zen_grupo,
	trim(collaborator_ids) cod_zen_usuarios_colaboradores,
	trim(email_cc_ids) cod_zen_email_cc,
	case when trim(is_public)='True' then 1 else 0 end as flag_zen_publico,
	trim(tags) as ds_zen_etiquetas,
	trim(custom_fields) as ds_zen_campos_personalizados,
	get_json_object(regexp_replace(satisfaction_rating ,"\'","\"") ,'$.score') ds_zen_satisfaccion,
	trim(ticket_form_id) as cod_zen_form ,
	form.ds_zen_form,
	trim(brand_id) as cod_zen_brand,
	brand.ds_zen_brand,
	ticket.partition_date
from bi_corp_staging.zendesk_tk_puc_santander ticket
left join bi_corp_common.stk_zen_usuarios solicitante
on trim(ticket.requester_id)=solicitante.cod_zen_usuario
AND solicitante.partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Zendesk_Hist3') }}'
left join bi_corp_common.stk_zen_usuarios remitente
on trim(ticket.submitter_id)=remitente.cod_zen_usuario
AND remitente.partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Zendesk_Hist3') }}'
left join bi_corp_common.stk_zen_usuarios asignado
on trim(ticket.assignee_id)= asignado.cod_zen_usuario
AND asignado.partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Zendesk_Hist3') }}'
left join bi_corp_common.dim_zen_grupo grupo
on trim(group_id)=grupo.cod_zen_grupo
left join bi_corp_common.dim_zen_form form
on ticket.ticket_form_id=form.cod_zen_form
left join bi_corp_common.dim_zen_brand brand
on ticket.brand_id=brand.cod_zen_brand
WHERE ticket.partition_date= '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Zendesk_Hist3') }}'

UNION ALL

SELECT
	trim(url) ds_zen_url,
	trim(id) cod_zen_ticket,
	case when trim(external_id)='None' then null else trim(external_id) end as cod_zen_external,
	CASE WHEN regexp_replace(split(via,"\'")[3],"_==channel==","")='any' Then lower(regexp_replace(split(via,"\'")[25],"_==channel==",""))
		 ELSE regexp_replace(split(via,"\'")[3],"_==channel==","") END AS ds_zen_canal,
	regexp_replace(regexp_replace(created_at,"T"," "),"Z","") as ts_zen_creacion,
	regexp_replace(regexp_replace(updated_at,"T"," "),"Z","") as ts_zen_actualizacion,
	trim(subject) as ds_zen_asunto,
	trim(description) as ds_zen_ticket,
	case when trim(priority)='None' then null else trim(priority) end as ds_zen_prioridad,
	trim(status) as ds_zen_status,
	case when trim(recipient)='None' then null else trim(recipient) end as cod_zen_destinatario,
	trim(requester_id) as cod_zen_usuario_solicitante,
	solicitante.ds_zen_nombre as ds_zen_usuario_solicitante,
	trim(submitter_id) as cod_zen_usuario_remitente,
	remitente.ds_zen_nombre as ds_zen_usuario_remitente,
	trim(assignee_id) as cod_zen_usuario_asignado,
	asignado.ds_zen_nombre as ds_zen_usuario_asignado,
	case when trim(organization_id)='None' then null else trim(organization_id) end as cod_zen_organizacion,
	case when trim(group_id)='None' then null else trim(group_id) end as cod_zen_grupo,
	grupo.ds_zen_grupo,
	trim(collaborator_ids) cod_zen_usuarios_colaboradores,
	trim(email_cc_ids) cod_zen_email_cc,
	case when trim(is_public)='True' then 1 else 0 end as flag_zen_publico,
	trim(tags) as ds_zen_etiquetas,
	trim(custom_fields) as ds_zen_campos_personalizados,
	get_json_object(regexp_replace(satisfaction_rating ,"\'","\"") ,'$.score') ds_zen_satisfaccion,
	trim(ticket_form_id) as cod_zen_form ,
	form.ds_zen_form,
	trim(brand_id) as cod_zen_brand,
	brand.ds_zen_brand,
	ticket.partition_date
from bi_corp_staging.zendesk_tk_comex_santander ticket
left join bi_corp_common.stk_zen_usuarios solicitante
on trim(ticket.requester_id)=solicitante.cod_zen_usuario
AND solicitante.partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Zendesk_Hist3') }}'
left join bi_corp_common.stk_zen_usuarios remitente
on trim(ticket.submitter_id)=remitente.cod_zen_usuario
AND remitente.partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Zendesk_Hist3') }}'
left join bi_corp_common.stk_zen_usuarios asignado
on trim(ticket.assignee_id)= asignado.cod_zen_usuario
AND asignado.partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Zendesk_Hist3') }}'
left join bi_corp_common.dim_zen_grupo grupo
on trim(group_id)=grupo.cod_zen_grupo
left join bi_corp_common.dim_zen_form form
on ticket.ticket_form_id=form.cod_zen_form
left join bi_corp_common.dim_zen_brand brand
on ticket.brand_id=brand.cod_zen_brand
WHERE ticket.partition_date= '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Zendesk_Hist3') }}';