"set mapred.job.queue.name=root.dataeng;
select trx_account_id
,trx_account_name
,trx_amount
,trx_authorization_code
,trx_balance_amount
,trx_card_holder_document
,trx_card_holder_name
,to_Date(trx_chargeback_date)
,trx_commission_amount
,trx_commission_tax_amount
,trx_conciliated
,trx_cuit
,trx_cupon
,trx_currency_code
,trx_customer_email
,trx_deposit_id
,to_date(trx_fecha_disponibilidad)
,trx_financial_cost_amount
,trx_financial_cost_tax_amount
,trx_income_tax_amount
,trx_input_mode
,trx_installments
,trx_latitude
,trx_longitude
,trx_pan
,trx_payment_days
,trx_payment_method_name
,trx_product_id
,trx_product_name
,trx_province_id
,trx_reader_serial_number
,trx_reference_number
,trx_refund_transaction_quantity
,trx_response
,trx_sales_transaction_quantity
,trx_sequence_number
,trx_site_id
,trx_subsidiary_id
,trx_subsidiary_name
,trx_subsidiary_province
,trx_tax_amount
,trx_tip_amount
,to_date(trx_transaction_date)
,trx_transaction_id
,trx_transaction_status
,to_date(trx_transferred_at)
,trx_type
,trx_user_name
,trx_vat_amount
,trx_voucher_emails
,to_date(partition_date)
from bi_corp_staging.getnet_transactions
where partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='GETNET_REPORT') }}'
and to_date(trx_transaction_date)  = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='GETNET_REPORT') }}'
"