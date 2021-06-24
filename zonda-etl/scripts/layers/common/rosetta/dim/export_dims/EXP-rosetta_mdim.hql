"
SET mapred.job.queue.name=root.dataeng;
select  domain_code,
        segmentation_code,
        attribute_code,
        name,
        second_name,
        hierarchical_level,
        end_date,
        unit_code
from bi_corp_common.rosetta_mdim
where partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='last_calendar_day', dag_id='LOAD_BDR_Rosetta_Export_MDIM_TDIM') }}'
"