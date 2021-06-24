set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=4000000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.exec.dynamic.partition.mode=nonstrict;
set mapred.job.queue.name=root.dataeng;

CREATE TEMPORARY TABLE max_partition_motivo AS
select max(partition_date) as max_partition_date
from bi_corp_staging.alcen_tmotivo_sw_srs e
where e.partition_date > date_sub('{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Admision_Ind-Daily') }}', 7)
;

INSERT OVERWRITE TABLE bi_corp_common.dim_adm_ind_motivoswsrs
PARTITION (partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Admision_Ind-Daily') }}')

select
trim(cod_grupo_decision) as cod_adm_grupodecision,
trim(cod_motivo) as cod_adm_motivo,
cast(nro_prioridad as int) as fc_adm_nroprioridad,
trim(cod_motivo_asol) as cod_adm_motivoasol,
trim(nom_aplicativo) as ds_adm_nomaplicativo,
trim(des_motivo) as ds_adm_motivo
from bi_corp_staging.alcen_tmotivo_sw_srs e
inner join max_partition_motivo m on e.partition_date= m.max_partition_date
;