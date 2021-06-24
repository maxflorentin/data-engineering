set mapred.job.queue.name=root.dataeng;
SET hive.exec.dynamic.partition.mode=nonstrict;

INSERT  OVERWRITE TABLE bi_corp_common.bt_cc_genesysdiscador
PARTITION (partition_date)


select
DISTINCT record_id													id_cc_registro,
from_unixtime(cast(`time`as bigint)-3*3600)					ts_cc_fecha,
`action`													cod_cc_accion,
CASE WHEN `action` ='1' THEN "DA_CALL_DIALED_OUTBOUND"
		WHEN `action` ='2' THEN "DA_CALL_DIALED_PREVIEW"
		WHEN `action` ='3' THEN "DA_CALL_DIALED_CALLBACK"
		WHEN `action` ='4' THEN "DA_CALL_DIALED_CHAIN"
		WHEN `action` ='5' THEN "DA_RECORD_APPLY_TREATMENT"
		WHEN `action` ='6' THEN "DA_CALL_QUEUED"
		WHEN `action` ='7' THEN "DA_CALL_ESTABLISHED"
		WHEN `action` ='8' THEN "DA_RECORD_REALEASED"
		WHEN `action` ='9' THEN "DA_RECORD_RESCHEDULED"
		WHEN `action` ='10' THEN "DA_RECORD_UPDATED"
		WHEN `action` ='11' THEN "DA_RECORD_PROCESSED"
		WHEN `action` ='12' THEN "DA_CALL_COMPLETED"
		WHEN `action` ='13' THEN "DA_CALL_TRANSFERED"
		WHEN `action` ='14' THEN "DA_RECORD_PROCESSED_EVENT"
		ELSE NULL END										ds_cc_accion,
record_handle												cod_cc_genesys,
list_id														cod_cc_lista,
list.name													ds_cc_lista,
campaign_id													cod_cc_campania,
camp.name													ds_cc_campania,
group_id													cod_cc_grupo,
trim(gr.name)													    ds_cc_grupo,
ocs_app_id													cod_cc_ocs_app,
tenant_id													cod_cc_inquilino,
trim(ten.name)											ds_cc_inquilino,
connection_id												cod_cc_conexion,
phone_type													cod_cc_tipo_telefono,
phone														ds_cc_telefono,
record_type													cod_cc_tipo_registro,
record_status												cod_cc_estado_registro,
call_result													cod_cc_resultado_llamada,
cod.descrip													ds_cc_resultado_llamada,
attempt														ds_cc_num_intentos,
CASE WHEN dial_sched_time='0' THEN NULL
ELSE from_unixtime(cast(dial_sched_time as bigint)) END 	ts_cc_llamada_programada,
from_unixtime(cast(call_time as bigint)) 					ts_cc_llamada,
substring(from_unixtime(cast(daily_from as bigint)),12) 	ds_cc_diariamente_desde,
substring(from_unixtime(cast(daily_till as bigint)),12)  	ds_cc_diariamente_hasta,
agent_id													ds_cc_agente,
chain_id													cod_cc_cadena,
customdata													ds_cc_datosm,
table_name													ds_cc_nombre_tabla,
dis.partition_Date

from bi_corp_staging.genesys_discador_cv dis
left join bi_corp_staging.rio46_cfg_calling_list list on (trim(dis.list_id)=trim(list.dbid) and list.partition_date=IF("{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_CONTACT_CENTER') }}" < '2020-12-13','2020-12-13',"{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_CONTACT_CENTER') }}"))
left join bi_corp_staging.rio46_cfg_campaign camp on (trim(dis.campaign_id)=trim(camp.dbid) and camp.partition_date=IF("{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_CONTACT_CENTER') }}" < '2020-12-13','2020-12-13',"{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_CONTACT_CENTER') }}"))
left join bi_corp_staging.rio46_cod_retornos cod on (trim(dis.call_result)=trim(cod.cod_retorno_genesys) and cod.partition_date=IF("{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_CONTACT_CENTER') }}" < '2020-12-13','2020-12-13',"{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_CONTACT_CENTER') }}"))
left join bi_corp_staging.rio46_cfg_tenant ten on (trim(dis.tenant_id)=trim(ten.dbid) and ten.partition_date=IF("{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_CONTACT_CENTER') }}" < '2020-12-14','2020-12-14',"{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_CONTACT_CENTER') }}"))
left join bi_corp_staging.rio46_cfg_group gr on (trim(dis.group_id)=trim(gr.dbid) and gr.partition_date=IF("{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_CONTACT_CENTER') }}" < '2020-12-13','2020-12-13',"{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_CONTACT_CENTER') }}"))
where dis.partition_date="{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_CONTACT_CENTER') }}";