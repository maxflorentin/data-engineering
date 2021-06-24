"SET mapred.job.queue.name=root.dataeng;
select 'AR' reporting_country,
	    '{{ ti.xcom_pull(task_ids='InputConfig', key='previous_month', dag_id='REPORT_CMIS_BSPL') }}' period_id,
        'V1.1' scope_id,
	    'V2.2' perimeter_id,
	    'V3.1' temporary_cut_id,
	    'V4.2' calendar_id,
        lpad('83', 5, '000') legal_entity_id,
	    'ARS' currency_id,
	    pc.codigo_estructural,
	         translate(cast(sum(case
	          when month(to_date('{{ ti.xcom_pull(task_ids='InputConfig', key='previous_month_from', dag_id='REPORT_CMIS_BSPL') }}')) = 1 then
	                    sc.imp_fin_enero
	          when month(to_date('{{ ti.xcom_pull(task_ids='InputConfig', key='previous_month_from', dag_id='REPORT_CMIS_BSPL') }}')) = 2 then
	                    sc.imp_fin_febrero
	          when month(to_date('{{ ti.xcom_pull(task_ids='InputConfig', key='previous_month_from', dag_id='REPORT_CMIS_BSPL') }}')) = 3 then
	                    sc.imp_fin_marzo
   		      when month(to_date('{{ ti.xcom_pull(task_ids='InputConfig', key='previous_month_from', dag_id='REPORT_CMIS_BSPL') }}')) = 4 then
   		                sc.imp_fin_abril
   		      when month(to_date('{{ ti.xcom_pull(task_ids='InputConfig', key='previous_month_from', dag_id='REPORT_CMIS_BSPL') }}')) = 5 then
   		                sc.imp_fin_mayo
   		      when month(to_date('{{ ti.xcom_pull(task_ids='InputConfig', key='previous_month_from', dag_id='REPORT_CMIS_BSPL') }}')) = 6 then
   		                sc.imp_fin_junio
   		      when month(to_date('{{ ti.xcom_pull(task_ids='InputConfig', key='previous_month_from', dag_id='REPORT_CMIS_BSPL') }}')) = 7 then
   		                sc.imp_fin_julio
   		      when month(to_date('{{ ti.xcom_pull(task_ids='InputConfig', key='previous_month_from', dag_id='REPORT_CMIS_BSPL') }}')) = 8 then
   		                sc.imp_fin_agosto
   		      when month(to_date('{{ ti.xcom_pull(task_ids='InputConfig', key='previous_month_from', dag_id='REPORT_CMIS_BSPL') }}')) = 9 then
   		                sc.imp_fin_setiembre
   		      when month(to_date('{{ ti.xcom_pull(task_ids='InputConfig', key='previous_month_from', dag_id='REPORT_CMIS_BSPL') }}')) = 10 then
   		                sc.imp_fin_octubre
   		      when month(to_date('{{ ti.xcom_pull(task_ids='InputConfig', key='previous_month_from', dag_id='REPORT_CMIS_BSPL') }}')) = 11 then
   		                sc.imp_fin_noviembre
   		      when month(to_date('{{ ti.xcom_pull(task_ids='InputConfig', key='previous_month_from', dag_id='REPORT_CMIS_BSPL') }}')) = 12 then
   		                sc.imp_fin_diciembre
   		      end) * -1 as string),'.',',') amount
from santander_business_risk_arda.saldos_consolidados sc, bi_corp_staging.cmis_parametria_bspl pc
where cast(sc.plan_numero_009 as string) like concat(pc.codigo_agrupamiento_rubro,'%')
and sc.data_date_part =  '{{ ti.xcom_pull(task_ids='InputConfig', key='last_working_day_arda', dag_id='REPORT_CMIS_BSPL') }}'
and pc.tipo = 'PL'
group by pc.codigo_estructural, pc.codigo_agrupamiento_rubro"