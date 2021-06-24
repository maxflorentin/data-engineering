set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=4000000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.exec.dynamic.partition.mode=nonstrict;
SET mapred.job.queue.name=root.dataeng;


INSERT OVERWRITE TABLE bi_corp_common.stk_adm_ind_tramiteshash
PARTITION (partition_date= '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Admision_Ind-History') }}')

select
trim(e.tipo_tramite) as cod_adm_tipotramite,
trim(e.cod_tramite) as cod_adm_tramite,
trim(e.codigo_hash) as cod_adm_hash,
trim(e.valor) as cod_adm_valor
from bi_corp_staging.alcen_ttramites_hash e
where e.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Admision_Ind-History') }}'
;