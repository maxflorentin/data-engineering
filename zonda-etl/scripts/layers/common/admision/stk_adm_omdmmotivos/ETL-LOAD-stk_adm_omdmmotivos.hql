set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=4000000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.exec.dynamic.partition.mode=nonstrict;
set mapred.job.queue.name=root.dataeng;


INSERT OVERWRITE TABLE bi_corp_common.stk_adm_omdmmotivos
    PARTITION (partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Admision') }}')
select tipo_tramite as cod_adm_tipotramite,
cod_tramite as cod_adm_tramite,
cast(nro_index as int) cod_adm_index,
des_result as ds_adm_desresult,
des_status as ds_adm_desstatus,
des_nombre as ds_adm_desnombre,
des_tipo as ds_adm_destipo,
cast(nro_rank as int) as cod_adm_rank,
cod_decision as cod_adm_decision,
cod_motivo as cod_adm_motivo,
substr(fec_proceso,1,19) as ts_adm_fecproceso
from bi_corp_staging.scoring_omdm_motivos
where partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Admision') }}';