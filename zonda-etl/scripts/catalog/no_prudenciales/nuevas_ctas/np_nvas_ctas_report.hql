"
set mapred.job.queue.name=root.dataeng;
create temporary table migrate_accounts as
select   concat(mig_new_entidad,mig_new_cent_alta,mig_new_cuenta) as mig_new_cuenta from
    bi_corp_staging.malbgc_zbdtmig
where partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='REPORT_NP_NVAS_CTAS') }}'
group by mig_new_entidad,mig_new_cent_alta,mig_new_cuenta
;

select count(distinct concat(aux.entidad,aux.centro_alta,aux.cuenta)) from bi_corp_staging.bgdtaux aux
left join migrate_accounts ma on ma.mig_new_cuenta=concat(aux.entidad,aux.centro_alta,aux.cuenta)
where partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='REPORT_NP_NVAS_CTAS') }}'
and  aux.fecha_alta >= date_sub('{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='REPORT_NP_NVAS_CTAS') }}', 90)
and ma.mig_new_cuenta is null
;
"