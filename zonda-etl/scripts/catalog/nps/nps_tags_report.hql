"set mapred.job.queue.name=root.dataeng;
select *
from bi_corp_staging.qualtrics_nps_tags
where partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='previous_process_date', dag_id='NPS_TAGS_REPORT') }}'
"