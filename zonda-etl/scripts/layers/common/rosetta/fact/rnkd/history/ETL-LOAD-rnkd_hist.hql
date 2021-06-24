SET mapred.job.queue.name=root.dataeng;
SET hive.exec.dynamic.partition.mode=nonstrict;

-- inserto rnkd del mes completo en historica
INSERT OVERWRITE TABLE bi_corp_common.rosetta_rnkd_hist
PARTITION (partition_date,
           domain_code)
select legal_entity_code,
	   native_key,
	   segmentation_code,
	   attribute_code,
	   local_value,
	   global_value,
	   end_date,
	   partition_date,
	   domain_code
from bi_corp_common.rosetta_rnkd
where partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='last_calendar_day', dag_id='LOAD_BDR_Rosetta_Save_History') }}';
