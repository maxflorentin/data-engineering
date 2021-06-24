set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=4000000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.exec.dynamic.partition.mode=nonstrict;
set mapred.job.queue.name=root.dataeng;

CREATE TEMPORARY TABLE max_partition_concesionario AS
select max(partition_date) as max_partition_date
from bi_corp_staging.alcen_tconcesionario e
where e.partition_date > date_sub('{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Admision_Ind-Daily') }}', 7)
;

INSERT OVERWRITE TABLE bi_corp_common.dim_adm_ind_concesionario
PARTITION (partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Admision_Ind-Daily') }}')

select
trim(cod_concesionario) as cod_adm_concesionario,
trim(nom_concesionario) as ds_adm_nomconcesionario,
trim(fec_aprobacion) as ts_adm_fecaprobacion,
trim(mar_resol_comite) as ds_adm_marresolcomite,
trim(mar_tienda) as ds_adm_martienda
from bi_corp_staging.alcen_tconcesionario e
inner join max_partition_concesionario m on e.partition_date= m.max_partition_date
;

