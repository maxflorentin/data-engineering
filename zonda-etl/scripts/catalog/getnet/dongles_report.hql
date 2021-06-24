" set mapred.job.queue.name=root.dataeng;
select dng_name
,dng_legal_name
,dng_cuit
,dng_type
,dng_address
,dng_postal_code
,dng_province
,dng_city
,dng_created_at
,dng_first_name
,dng_last_name
,dng_document_number
,dng_email
,dng_phone
,dng_category
,dng_id_externo
,dng_codigo_canal
,dng_transaction_date
from bi_corp_staging.getnet_dongles
where partition_date = DATE_SUB('{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='GETNET_REPORT') }}', 8)
"