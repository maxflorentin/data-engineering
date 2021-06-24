set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=4000000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.exec.dynamic.partition.mode=nonstrict;
set mapred.job.queue.name=root.dataeng;

INSERT OVERWRITE TABLE bi_corp_common.stk_adm_omdm
    PARTITION (partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Admision') }}')
select
tipo_tramite as cod_adm_tipotramite,
cod_tramite as cod_adm_tramite,
substr(fec_proceso,1,19) as ts_adm_fecproceso,
substr(fec_request,1,19) as ts_adm_fecrequest,
substr(fec_response,1,19) as ts_adm_fecresponse,
substr(fec_fin_proc,1,19) as ts_adm_fecfinproc,
des_observacion as ds_adm_desobservacion,
cod_decision as cod_adm_decision,
tipo_decision as ds_adm_tipodecision,
des_nombre_flujo as ds_adm_desnombreflujo,
des_ultimo_paso as ds_adm_desultimopaso,
des_categoria_producto as ds_adm_descategoriaproducto,
des_cod_producto as ds_adm_descodproducto,
indicador_estado as ds_adm_indicadorestado,
des_nombre_estrategia as ds_adm_desnombreestrategia
from bi_corp_staging.scoring_omdm
where partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Admision') }}';
