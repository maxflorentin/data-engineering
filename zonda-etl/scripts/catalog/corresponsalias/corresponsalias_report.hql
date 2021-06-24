"set mapred.job.queue.name=root.dataeng;
select
  cor_correspondent ,
  cor_serviceName ,
  cor_accountNumber,
  cor_nup,
  TO_DATE(cor_operationDate),
  cor_creditCardNumber,
  cor_amount,
  cor_subsidiaryId,
  cor_subsidiaryName
 from bi_corp_staging.corresponsalias_nps
where partition_date =  '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='CORRESPONSALIAS_REPORT') }}'
and  TO_DATE(cor_operationDate) =  '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='CORRESPONSALIAS_REPORT') }}'
and  cor_servicename in ('Depósito','Extracción','Pago de TC','Pagó de Préstamo')
"