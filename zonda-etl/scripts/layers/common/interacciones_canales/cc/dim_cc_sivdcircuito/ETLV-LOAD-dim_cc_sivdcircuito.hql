SET mapred.job.queue.name=root.dataeng;
SET hive.exec.dynamic.partition.mode=nonstrict;

INSERT  overwrite TABLE bi_corp_common.dim_cc_sivdcircuito PARTITION (partition_date)

Select
DISTINCT cir.codigo as cod_cc_circuito,
descri as ds_cc_circuito,
dias_backoffice as fc_cc_diasbackoffice,
CASE WHEN activo ='S' THEN 1 ELSE 0 END as flag_cc_activo,
tipo_informe_entrevista as cod_cc_tipoentrevista,
codificacion_oca as cod_cc_oca,
pais as cod_cc_pais,
CASE WHEN tiposolicitud = 'null' THEN  NULL ELSE TRIM(tiposolicitud) END  as cod_cc_tiposolicitud,
CASE WHEN procesotibco = 'null' THEN  NULL ELSE TRIM(procesotibco) END  as ds_cc_procesotibco,
CASE WHEN tipocircuito = 'null' THEN  NULL ELSE TRIM(tipocircuito) END  as cod_cc_tipocircuito,
tip.descripcion                                                                    as ds_cc_tipocircuito,
CASE WHEN reversa ='S' THEN 1 ELSE 0 END as flag_cc_reversa,
cir.partition_date

from bi_corp_staging.rio3_circuito cir
left join bi_corp_staging.rio3_tmk_tipocircuito tip on ( tip.codigo= cir.tipocircuito and tip.partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_CONTACT_CENTER') }}')
Where cir.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_CONTACT_CENTER') }}';
