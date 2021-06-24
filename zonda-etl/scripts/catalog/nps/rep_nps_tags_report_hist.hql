"SET mapred.job.queue.name=root.dataeng;
select *
from bi_corp_staging.qualtrics_nps_tags
where partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='NPS_REP_TAGS_REPORT_HIST') }}'
"