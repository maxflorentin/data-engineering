"set mapred.job.queue.name=root.dataeng;
select
mer.mer_id_externo,
mer.mer_cuit,
mer.mer_document_number,
mer.mer_email,
mer.mer_first_name,
mer.mer_last_name,
mer.mer_legal_name,
mer.mer_name,
mer.mer_phone,
mer.mer_city,
mer.mer_province,
to_date(mer.mer_created_at) mer_created_at,
mer.mer_codigo_canal,
mer.mer_category,
mer.mer_sucursal_alta,
tk.id tk_id,
REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(split(tk.via,'==')[2],':',''), '\'', ''), ',', '')  via,
to_date(tk.created_at) tk_created_at,
to_date(tk.updated_at) tk_updated_at,
tk.subject tk_subject,
tk.raw_subject tk_raw_subject,
substr(tk.description, 1, 3200) as tk_description,
tk.status tk_status,
tk.recipient tk_recipient,
tk.requester_id tk_requester_id,
tk.submitter_id tk_submitter_id,
tk.assignee_id tk_assignee_id,
tk.ticket_form_id tk_ticket_form_id,
col_us.usr_id,
col_us.usr_name,
col_us.usr_email,
to_date(col_us.usr_created_at) usr_created_at,
to_date(col_us.usr_updated_at) usr_updated_at,
col_us.usr_phone,
col_us.usr_role,
col_us.usr_external_id,
col_us.usr_user_fields,
col_us.partition_date usr_partition_date,
col_ag.usr_id col_id,
col_ag.usr_name col_name,
col_ag.usr_email col_email,
col_ag.usr_role col_role,
col_ag.partition_date col_partition_date
from bi_corp_staging.getnet_merchants as mer,
bi_corp_staging.zendesk_tk_getnet as tk
INNER JOIN (select usr_id,usr_name,usr_email,usr_created_at,usr_updated_at,usr_phone,usr_role,
                   usr_external_id,usr_user_fields,partition_date,ROW_NUMBER() OVER (PARTITION BY usr_id ORDER BY partition_date DESC) as orden
from bi_corp_staging.zendesk_usr_getnet) as col_us
on (tk.requester_id = col_us.usr_id and col_us.usr_role = 'end-user' and col_us.orden = 1)
LEFT JOIN (select usr_id,usr_name,usr_email,usr_role,partition_date,ROW_NUMBER() OVER (PARTITION BY usr_id ORDER BY partition_date DESC) as orden
from bi_corp_staging.zendesk_usr_getnet) as col_ag
on (tk.assignee_id = col_ag.usr_id and col_ag.orden = 1)
where mer.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='GETNET_ZEND_CUST_SERV_REPORT') }}'
and mer.mer_zendesk_id = tk.requester_id
and tk.requester_id = tk.submitter_id
and tk.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='GETNET_ZEND_CUST_SERV_REPORT') }}'
and tk.status = 'closed'
"