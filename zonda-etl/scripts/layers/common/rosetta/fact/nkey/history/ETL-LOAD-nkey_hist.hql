SET mapred.job.queue.name=root.dataeng;
SET hive.exec.dynamic.partition.mode=nonstrict;

-- inserto nkey del mes completo en historica
INSERT OVERWRITE TABLE bi_corp_common.rosetta_nkey_hist
PARTITION (partition_date,
           domain_code)
select legal_entity_code,
	   native_key,
	   master_key,
	   end_date,
	   partition_date,
	   domain_code
from bi_corp_common.rosetta_nkey
where partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='last_calendar_day', dag_id='LOAD_BDR_Rosetta_Save_History') }}';
