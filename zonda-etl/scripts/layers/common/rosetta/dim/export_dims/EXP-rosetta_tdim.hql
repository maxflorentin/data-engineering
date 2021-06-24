"
SET mapred.job.queue.name=root.dataeng;
select domain_code,
	   segmentation_code,
	   attribute_code,
	   dimension_value,
	   vision,
case   when name = 'null'
	   	then ''
	   else
	   	regexp_replace(name,',','')
	   end name,
case   when second_name = 'null'
	   	then ''
	   else
	   	regexp_replace(second_name,',','')
	   end second_name,
	   nvl(father_attribute_code,'') father_attribute_code,
	   nvl(father_dimension_value,'') father_dimension_value,
	   end_date,
	   unit_code
from bi_corp_common.rosetta_tdim
where partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='last_calendar_day', dag_id='LOAD_BDR_Rosetta_Export_MDIM_TDIM') }}'
"