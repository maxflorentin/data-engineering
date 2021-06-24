"set mapred.job.queue.name=root.dataeng;
select tk.url
,tk.id
,'CHAT'  as via
,to_date(tk.created_at) as created_at
,to_date(tk.updated_at) as updated_at
,tk.subject
,tk.raw_subject
,substr(tk.description, 1, 3200) as description
,tk.status
,tk.requester_id
,tk.assignee_id
,tk.group_id
,tk.tags
,tk.ticket_form_id
,tk.brand_id
,tk.RESULT_TYPE
,ar1.usr_id   id_solicitante
,ar1.usr_name  name_solicitante
,ar1.usr_email email_solicitante
,ar1.usr_url url_solicitante
,ar1.usr_phone phone_solicitante
,ar1.usr_external_id nup_solicitante
,ar1.partition_date as patition_solicitante
,ar2.usr_id as id_colaborador
,ar2.usr_name as name_colaborador
,ar2.usr_url as url_colaborador
,ar2.usr_user_fields
,trim(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(UCASE(split(ar2.usr_user_fields,'num_==legajo==')[1]), '_EJECUTIVO',''),'[*,:_]',''),'NUM','')) legajo
,ar2.usr_tags
,ar2.partition_date as partition_colaborador
from bi_corp_staging.zendesk_tk_santander_ar as tk
LEFT JOIN (select usr_id,usr_name, usr_url,usr_email, usr_tags,usr_user_fields,usr_phone, usr_external_id, partition_date, ROW_NUMBER() OVER (PARTITION BY usr_id ORDER BY partition_date DESC) as orden
from bi_corp_staging.zendesk_usr_santander_ar) as ar1
on (requester_id = ar1.usr_id and ar1.orden = 1)
LEFT JOIN (select usr_id,usr_name, usr_url,usr_email, usr_tags, usr_user_fields, partition_date, ROW_NUMBER() OVER (PARTITION BY usr_id ORDER BY partition_date DESC) as orden
from bi_corp_staging.zendesk_usr_santander_ar) as ar2
on (assignee_id = ar2.usr_id and ar2.orden = 1)
where tk.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='ZENDESK_EJECUTIVO_REPORT') }}'
and to_date(updated_at) = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='ZENDESK_EJECUTIVO_REPORT') }}'
and status = 'closed'
and UCASE(split(tk.via,'==')[2]) LIKE ('%CHAT%')
and UCASE(tk.tags) not like ('%CONTINGENCIA%')
UNION ALL
select tk.url
,tk.id
,'WHATSAPP' via
,to_date(tk.created_at) as created_at
,to_date(tk.updated_at) as updated_at
,tk.subject
,tk.raw_subject
,substr(tk.description, 1, 3200) as description
,tk.status
,tk.requester_id
,tk.assignee_id
,tk.group_id
,tk.tags
,tk.ticket_form_id
,tk.brand_id
,tk.RESULT_TYPE
,ar1.usr_id   id_solicitante
,ar1.usr_name  name_solicitante
,ar1.usr_email email_solicitante
,ar1.usr_url url_solicitante
,ar1.usr_phone phone_solicitante
,ar1.usr_external_id nup_solicitante
,ar1.partition_date as patition_solicitante
,ar2.usr_id as id_colaborador
,ar2.usr_name as name_colaborador
,ar2.usr_url as url_colaborador
,ar2.usr_user_fields
,trim(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(UCASE(split(ar2.usr_user_fields,'num_==legajo==')[1]), '_EJECUTIVO',''),'[*,:_]',''),'NUM','')) legajo
,ar2.usr_tags
,ar2.partition_date as partition_colaborador
from bi_corp_staging.zendesk_tk_santander_ar as tk
LEFT JOIN (select usr_id,usr_name, usr_url,usr_email, usr_tags,usr_user_fields,usr_phone, usr_external_id, partition_date, ROW_NUMBER() OVER (PARTITION BY usr_id ORDER BY partition_date DESC) as orden
from bi_corp_staging.zendesk_usr_santander_ar) as ar1
on (requester_id = ar1.usr_id and ar1.orden = 1)
LEFT JOIN (select usr_id,usr_name, usr_url,usr_email, usr_tags, usr_user_fields, partition_date, ROW_NUMBER() OVER (PARTITION BY usr_id ORDER BY partition_date DESC) as orden
from bi_corp_staging.zendesk_usr_santander_ar) as ar2
on (assignee_id = ar2.usr_id and ar2.orden = 1)
where tk.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='ZENDESK_EJECUTIVO_REPORT') }}'
and to_date(updated_at) = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='ZENDESK_EJECUTIVO_REPORT') }}'
and status = 'closed'
and UCASE(TK.VIA) LIKE ('%MESSAGING%')
and UCASE(tk.tags) not like ('%CONTINGENCIA%')
union all
select tk.url
,tk.id
,'EMAIL'  as via
,to_date(tk.created_at) as created_at
,to_date(tk.updated_at) as updated_at
,tk.subject
,tk.raw_subject
,substr(tk.description, 1, 3200) as description
,tk.status
,tk.requester_id
,tk.assignee_id
,tk.group_id
,tk.tags
,tk.ticket_form_id
,tk.brand_id
,tk.RESULT_TYPE
,ar1.usr_id   id_solicitante
,ar1.usr_name  name_solicitante
,ar1.usr_email email_solicitante
,ar1.usr_url url_solicitante
,ar1.usr_phone phone_solicitante
,ar1.usr_external_id nup_solicitante
,ar1.partition_date as patition_solicitante
,ar2.usr_id as id_colaborador
,ar2.usr_name as name_colaborador
,ar2.usr_url as url_colaborador
,ar2.usr_user_fields
,trim(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(UCASE(split(ar2.usr_user_fields,'num_==legajo==')[1]), '_EJECUTIVO',''),'[*,:_]',''),'NUM','')) legajo
,ar2.usr_tags
,ar2.partition_date as partition_colaborador
from bi_corp_staging.zendesk_tk_santander_ar as tk
LEFT JOIN (select usr_id,usr_name, usr_url,usr_email, usr_tags,usr_user_fields,usr_phone, usr_external_id, partition_date, ROW_NUMBER() OVER (PARTITION BY usr_id ORDER BY partition_date DESC) as orden
from bi_corp_staging.zendesk_usr_santander_ar) as ar1
on (requester_id = ar1.usr_id and ar1.orden = 1)
LEFT JOIN (select usr_id,usr_name, usr_url,usr_email, usr_tags, usr_user_fields, partition_date, ROW_NUMBER() OVER (PARTITION BY usr_id ORDER BY partition_date DESC) as orden
from bi_corp_staging.zendesk_usr_santander_ar) as ar2
on (assignee_id = ar2.usr_id and ar2.orden = 1)
where tk.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='ZENDESK_EJECUTIVO_REPORT') }}'
and to_date(updated_at) = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='ZENDESK_EJECUTIVO_REPORT') }}'
and status = 'closed'
and UCASE(split(tk.via,'==')[2]) LIKE ('%EMAIL%')
and UCASE(tk.tags) not like ('%CONTINGENCIA%')
"