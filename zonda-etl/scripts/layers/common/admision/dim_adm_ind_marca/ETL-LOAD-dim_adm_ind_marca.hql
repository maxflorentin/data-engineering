set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=4000000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.exec.dynamic.partition.mode=nonstrict;
SET mapred.job.queue.name=root.dataeng;

CREATE TEMPORARY TABLE max_partition_marca AS
select max(partition_date) as max_partition_date
from bi_corp_staging.alcen_tmarca e
where e.partition_date > date_sub('{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Admision_Ind-Daily') }}', 7)
;

INSERT OVERWRITE TABLE bi_corp_common.dim_adm_ind_marca
PARTITION (partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Admision_Ind-Daily') }}')

select
cast(cod_marca as int) as cod_adm_marca,
trim(des_marca) as ds_adm_marca
from bi_corp_staging.alcen_tmarca e
inner join max_partition_marca m on e.partition_date= m.max_partition_date
;
