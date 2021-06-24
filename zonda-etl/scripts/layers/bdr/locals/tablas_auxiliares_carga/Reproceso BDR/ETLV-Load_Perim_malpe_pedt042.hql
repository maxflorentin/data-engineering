set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=4000000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.exec.dynamic.partition.mode=nonstrict;
set mapred.job.queue.name=root.dataeng;

insert overwrite table bi_corp_bdr.perim_malpe_pedt042
partition(partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='max_partition_malpe_pedt042', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201912_202103') }}')
select pemotest as pemotest, 
       pecdgent as pecdgent,
       pecodofi as pecodofi,
       penumcon as penumcon,
       pecodpro as pecodpro,
       pecodsub as pecodsub,
       peestope as peestope, 
       pefecest as pefecest
from bi_corp_staging.malpe_pedt042
where partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='max_partition_malpe_pedt042', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201912_202103') }}' 
	and peestope = 'C'