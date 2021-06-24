"set mapred.job.queue.name=root.dataeng;
select tk.url
,tk.id
,REGEXP_REPLACE(split(tk.via,'==')[2],':','')  as via
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
,puc1.usr_id   id_solicitante
,puc1.usr_name  name_solicitante
,puc1.usr_email email_solicitante
,puc1.usr_url url_solicitante
,REGEXP_REPLACE(REGEXP_REPLACE(split(puc1.usr_user_fields,'==')[2],':',''),',','') as Legajo_solicitante
,REGEXP_REPLACE(REGEXP_REPLACE(split(puc1.usr_user_fields,'==')[14],':',''),'}','') as sucursal_solicitante
,REGEXP_REPLACE(REGEXP_REPLACE(split(puc1.usr_user_fields,'==')[12],':',''),',','')  as rol_solicitante
,puc1.partition_date as patition_solicitante
,puc2.usr_id as id_colaborador
,puc2.usr_name as name_colaborador
,puc2.usr_url as url_colaborador
,REGEXP_REPLACE(REGEXP_REPLACE(split(puc2.usr_user_fields,'==')[2],':',''),',','')  as Legajo_colaborador
,REGEXP_REPLACE(REGEXP_REPLACE(split(puc2.usr_user_fields,'==')[12],':',''),',','') as rol_colaborador
,puc2.partition_date as partition_colaborador
from bi_corp_staging.zendesk_tk_PUC_santander as tk
LEFT JOIN (select usr_id,usr_name, usr_url,usr_email, usr_tags,usr_user_fields,usr_phone, usr_external_id, partition_date, ROW_NUMBER() OVER (PARTITION BY usr_id ORDER BY partition_date DESC) as orden
from bi_corp_staging.zendesk_usr_puc_santander) as puc1
on (requester_id = puc1.usr_id and puc1.orden = 1)
LEFT JOIN (select usr_id,usr_name, usr_url,usr_email, usr_tags, usr_user_fields, partition_date, ROW_NUMBER() OVER (PARTITION BY usr_id ORDER BY partition_date DESC) as orden
from bi_corp_staging.zendesk_usr_puc_santander) as puc2
on (assignee_id = puc2.usr_id and puc2.orden = 1)
where tk.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='ZENDESK_REPORT') }}'
and to_date(updated_at) = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='ZENDESK_REPORT') }}'
and status = 'solved'
and tk.group_id not in ('360013313692', '360013314372')
and recipient not in ('hola@santander.com.ar', 'servicios_generales@hola-santander.zendesk.com', 'support@hola-santander.zendesk.com')
and UCASE(split(tk.via,'==')[2]) not LIKE ('%EMAIL%') and UCASE(split(tk.via,'==')[2]) not LIKE ('%WEB%')
"