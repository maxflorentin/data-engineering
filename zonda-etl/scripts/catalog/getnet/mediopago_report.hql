"set mapred.job.queue.name=root.dataeng;
select
 getnet_merchants.mer_NAME
,getnet_merchants.mer_LEGAL_NAME
,getnet_merchants.mer_CUIT
,getnet_merchants.mer_TYPE
,getnet_merchants.mer_STREET
,getnet_merchants.mer_STREET_NUMBER
,getnet_merchants.mer_APARTMENT
,getnet_merchants.mer_FLOOR
,getnet_merchants.mer_POSTAL_CODE
,getnet_merchants.mer_PROVINCE
,getnet_merchants.mer_PROVINCE_ID
,getnet_merchants.mer_CITY
,to_date(getnet_merchants.mer_CREATED_AT)
,getnet_merchants.mer_FIRST_NAME
,getnet_merchants.mer_LAST_NAME
,getnet_merchants.mer_DOCUMENT_NUMBER
,getnet_merchants.mer_EMAIL
,getnet_merchants.mer_PHONE
,getnet_merchants.mer_ACCOUNT_ENABLED
,getnet_merchants.mer_CATEGORY
,getnet_merchants.mer_SUBSIDIARY_ID
,getnet_merchants.mer_ACCOUNT_ID
,lpad(trim(getnet_merchants.mer_id_externo),8,'0')
,getnet_merchants.mer_SEXO
,getnet_merchants.mer_CODIGO_CANAL
,to_date(getnet_merchants.mer_FECHA_ALTA_EMPRESA)
,getnet_merchants.mer_SUCURSAL_ALTA
,getnet_transactions.trx_TRANSACTION_ID
,getnet_transactions.trx_TYPE
,to_date(getnet_transactions.trx_TRANSACTION_DATE)
,getnet_transactions.trx_ACCOUNT_NAME
,getnet_transactions.trx_SUBSIDIARY_NAME
,getnet_transactions.trx_user_name
,getnet_transactions.trx_AMOUNT
,to_date(getnet_transactions.trx_FECHA_DISPONIBILIDAD)
,getnet_transactions.trx_REFERENCE_NUMBER
,getnet_transactions.trx_INPUT_MODE
,getnet_transactions.trx_SUBSIDIARY_PROVINCE
,getnet_transactions.trx_INSTALLMENTS
,getnet_transactions.trx_TRANSACTION_STATUS
from bi_corp_staging.getnet_transactions , bi_corp_staging.getnet_merchants
where getnet_transactions.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='GETNET_REPORT') }}'
and getnet_merchants.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='GETNET_REPORT') }}'
and getnet_merchants.mer_cuit = getnet_transactions.trx_cuit
and to_date(getnet_transactions.trx_transaction_date) = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='GETNET_REPORT') }}'
and getnet_transactions.trx_cuit != '30716821516'
and getnet_transactions.trx_TRANSACTION_STATUS = 'APPROVED'
and getnet_merchants.mer_account_blocked <> '1'
"