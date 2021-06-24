"set mapred.job.queue.name=root.dataeng;
select operation_id,
cheque_id,
operation,
status,
target,
executed,
msg,
req,
res,
cuit,
emisor,
receptor,
posted
from bi_corp_staging.echeq_actions
where posted = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='ECHEQ_REPORT_RIO53') }}'
and LENGTH(trim(cuit)) < 12
"