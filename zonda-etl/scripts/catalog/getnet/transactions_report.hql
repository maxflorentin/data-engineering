" set mapred.job.queue.name=root.dataeng;
select trx_account_name
,trx_cuit
,trx_customer_email
,trx_subsidiary_name
,to_date(trx_transaction_date)
,trx_transaction_id
,trx_transaction_status
,trx_type
from bi_corp_staging.getnet_transactions
where partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='GETNET_REPORT') }}'
and to_date(trx_transaction_date)  = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='GETNET_REPORT') }}'
and trx_TRANSACTION_STATUS = 'APPROVED'
"