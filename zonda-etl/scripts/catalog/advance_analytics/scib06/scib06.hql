"set mapred.job.queue.name=root.dataeng;
select concat(lpad(cod_per_nup,8,0) ,
rpad(fecha_base,10,' '),
rpad(variable,50,' ') ,
rpad(descripcion,80,' ') ,
rpad(valor,50,' ') ,
rpad(valor_normalizado,5,0) )
from bi_corp_staging.mlops_aml_scib06
where partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='next_date_from', dag_id='REPORT_AA_SCIB06') }}' "
