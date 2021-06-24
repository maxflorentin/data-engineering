set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=4000000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.exec.dynamic.partition.mode=nonstrict;
set mapred.job.queue.name=root.dataeng;

CREATE TEMPORARY TABLE max_partition_modelo AS
select max(partition_date) as max_partition_date
from bi_corp_staging.alcen_tmodelo e
where e.partition_date > date_sub('{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Admision_Ind-Daily') }}', 7)
;

INSERT OVERWRITE TABLE bi_corp_common.dim_adm_ind_modelo
PARTITION (partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Admision_Ind-Daily') }}')

select
cast(cod_modelo as int) as cod_adm_modelo,
trim(des_modelo) as ds_adm_modelo
from bi_corp_staging.alcen_tmodelo e
inner join max_partition_modelo m on e.partition_date= m.max_partition_date


