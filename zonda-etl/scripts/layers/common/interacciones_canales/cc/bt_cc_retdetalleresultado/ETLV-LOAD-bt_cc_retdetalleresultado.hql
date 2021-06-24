set mapred.job.queue.name=root.dataeng;
SET hive.exec.dynamic.partition.mode=nonstrict;


INSERT OVERWRITE TABLE bi_corp_common.bt_cc_retdetalleresultado
PARTITION(partition_date)

select
DISTINCT re.operacion 													                            cod_cc_operacion,
cast(nuptitular as bigint)									                                cod_per_nup,
re.encuesta				    								                                cod_cc_encuesta,
en.descri													                                ds_cc_encuesta,
CASE WHEN en.activo='S' THEN 1 ELSE 0 END					                                flag_cc_encuesta_activa,
en.plantilla														                        ds_cc_encuesta_tipo,
re.producto												    								cod_cc_producto,
pro.descri																					ds_cc_producto,
re.pregunta																					cod_cc_pregunta,
pre.descri																					ds_cc_pregunta,
CASE WHEN pre.respuesta_multiple='S' or pre.respuesta_multiple= '1'
	 THEN 1 ELSE 0 END																		flag_cc_respuesta_multiple,
CASE WHEN pre.activo='S'  THEN 1 ELSE 0 END													flag_cc_pregunta_activa,
re.respuesta																				cod_cc_respuesta,
resp.descri																					ds_cc_respuesta	,
CASE WHEN resp.activo='S'  THEN 1 ELSE 0 END												flag_cc_respuesta_activa,
CASE WHEN trim(re.comentario)=''  THEN NULL ELSE trim(re.comentario) END					ds_cc_comentario,
re.partition_Date																			partition_Date

FROM bi_corp_staging.rio3_resultadoencuesta_rioline re
LEFT JOIN bi_corp_staging.rio3_operacionencuesta_rionline op on (
op.operacion = re.operacion
and op.producto = re.producto
and op.encuesta = re.encuesta
and op.partition_date="{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_CONTACT_CENTER-HIST') }}")
LEFT JOIN bi_corp_staging.rio3_tbl_encuesta en on (re.encuesta= en.codigo and en.partition_date="{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_CONTACT_CENTER') }}" )
LEFT JOIN bi_corp_staging.rio3_tbl_pregunta pre on (re.pregunta= pre.pregunta and re.encuesta=pre.encuesta and pre.partition_date="{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_CONTACT_CENTER') }}" )
LEFT JOIN bi_corp_staging.rio3_tbl_respuesta resp on (re.respuesta=resp.respuesta and re.encuesta=resp.encuesta
and re.pregunta=resp.pregunta and resp.partition_date= "{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_CONTACT_CENTER') }}")
LEFT JOIN bi_corp_staging.rio3_producto pro on (pro.codigo=re.producto and pro.partition_date='2021-04-28')
where re.partition_date="{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_CONTACT_CENTER') }}";

