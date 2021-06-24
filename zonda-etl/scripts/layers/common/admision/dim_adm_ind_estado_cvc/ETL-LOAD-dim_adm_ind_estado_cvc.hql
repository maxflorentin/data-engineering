set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=4000000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.exec.dynamic.partition.mode=nonstrict;
set mapred.job.queue.name=root.dataeng;

CREATE TEMPORARY TABLE max_partition_estado AS
select max(partition_date) as max_partition_date
from bi_corp_staging.cvc_testado e
where e.partition_date > date_sub('{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Admision_Ind-Daily') }}', 7)
;

INSERT OVERWRITE TABLE bi_corp_common.dim_adm_ind_estado_cvc
PARTITION (partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Admision_Ind-Daily') }}')

select
cast(cod_estado as int) as cod_adm_estado,
trim(des_estado) as ds_adm_estado,
trim(cod_sector) as cod_adm_sector,
trim(cod_estado_alcen) as cod_adm_estadoalcen,
case when trim(e.mar_egresado_final) = 'N' then 1 else 0 end as flag_adm_maregresadofinal
from bi_corp_staging.cvc_testado e
inner join max_partition_estado m on e.partition_date= m.max_partition_date
;