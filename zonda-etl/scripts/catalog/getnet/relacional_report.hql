"set mapred.job.queue.name=root.dataeng;
select mer_account_enabled
,mer_category
,TO_DATE(mer_created_at)
,mer_cuit
,mer_document_number
,lpad(trim(mer_id_externo),8,'0')
,mer_email
,mer_first_name
,mer_last_name
,mer_legal_name
,mer_name
,mer_phone
,mer_subsidiary_enabled
,mer_type
,mer_CODIGO_CANAL
,mer_SUCURSAL_ALTA
 from bi_corp_staging.getnet_merchants
where partition_date =  '{{ ti.xcom_pull(task_ids='InputConfig', key='previous_process_date', dag_id='GETNET_RELACIONAL_REPORT') }}'
and mer_account_blocked <> '1'
and mer_deleted_at is null
"