set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=4000000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.exec.dynamic.partition.mode=nonstrict;
set mapred.job.queue.name=root.dataeng;

insert overwrite table bi_corp_bdr.perim_mdr_contraparte 
partition(partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_Tables-Monthly') }}')
select distinct regexp_replace(k.alias_nup,'<NOT FOUND>','0') as nup
                ,trim(k.shortname) as shortname
from bi_corp_staging.mdr_contrapartes k 
where  k.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='max_partition_mdr_contrapartes', dag_id='BDR_LOAD_Tables-Monthly') }}'
and regexp_replace(k.alias_nup,'<NOT FOUND>','0') <> '0'
 