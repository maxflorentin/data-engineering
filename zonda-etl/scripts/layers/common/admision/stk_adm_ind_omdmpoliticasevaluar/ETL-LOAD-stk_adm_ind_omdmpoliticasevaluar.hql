set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=4000000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.exec.dynamic.partition.mode=nonstrict;
SET mapred.job.queue.name=root.dataeng;

INSERT OVERWRITE TABLE bi_corp_common.stk_adm_ind_omdmpoliticasevaluar
    PARTITION (partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Admision') }}')
select tipo_tramite as cod_adm_tipotramite,
cod_tramite as cod_adm_tramite,
substr(fec_proceso,1,19) as ts_adm_fecproceso,
b.politicaEvaluar as cod_adm_politicaevaluar,
json as ds_adm_json
from bi_corp_staging.scoring_omdm_solicitudes
lateral view json_tuple(json,
'politicaEvaluar'
) b as politicaEvaluar
where partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Admision') }}'
and label = 'PoliticasEvaluar';