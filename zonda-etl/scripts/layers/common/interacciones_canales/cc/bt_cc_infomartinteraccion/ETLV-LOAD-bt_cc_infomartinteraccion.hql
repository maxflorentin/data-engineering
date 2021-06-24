SET mapred.job.queue.name=root.dataeng;
SET hive.exec.dynamic.partition.mode=nonstrict;

INSERT  overwrite TABLE bi_corp_common.bt_cc_infomartinteraccion
PARTITION (partition_date)


select
DISTINCT TRANSLATE(interaction_id, ',','')																		cod_cc_interaccion,
fecha       			 											 									dt_cc_fecha,
intervalo				 											 									ds_cc_intervalo,
tipo_interaccion		 											 									ds_cc_tipo_interaccion,
canal					 											 									ds_cc_canal,
asunto					 											 									ds_cc_asunto,
CASE WHEN trim(last_queue)='NONE' then NULL else trim(last_queue) end   								ds_cc_last_queue,
CASE WHEN trim(last_vqueue)='NONE' then NULL else trim(last_vqueue) end 									ds_cc_last_vqueue,
`from`																 									ds_cc_from,
apellido																								ds_cc_apellido,
nombre																									ds_cc_nombre,
cast(nup as bigint)																						cod_per_nup,
cast(documento	as bigint)																				ds_cc_numdoc,
disposition_code																						ds_cc_cod_disposicion,
motivo_cierre																							ds_cc_motivo_cierre,
from_unixtime(unix_timestamp(inicio_gestion_interaccion,'dd/MM/yyyy HH:mm:ss'),'yyyy-MM-dd HH:mm:ss') 	ts_cc_inicio_gestioninteraccion,
from_unixtime(unix_timestamp(fin_gestion_interaccion,'dd/MM/yyyy HH:mm:ss'),'yyyy-MM-dd HH:mm:ss') 		ts_cc_fin_gestioninteraccion,
agente																									ds_cc_agente,
UPPER(employee_id)																						cod_cc_legajo,
from_unixtime(unix_timestamp(fin_interaccion,'dd MMM yyyy HH:mm:ss'),'yyyy-MM-dd HH:mm:ss') 			ts_cc_fin_interaccion,
from_unixtime(unix_timestamp(inicio_interaccion,'dd MMM yyyy HH:mm:ss'),'yyyy-MM-dd HH:mm:ss') 			ts_cc_inicio_interaccion,
partition_date																							partition_date
from  bi_corp_staging.infomart_detallado
Where partition_date= '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_CONTACT_CENTER') }}'
AND '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_CONTACT_CENTER') }}' >= '2020-10-01'

UNION ALL

select
TRANSLATE(interaction_id, ',','')																		cod_cc_interaccion,
from_unixtime(unix_timestamp(inicio_interaccion,'dd MMM yyyy HH:mm:ss'),'yyyy-MM-dd')       			dt_cc_fecha,
intervalo				 											 									ds_cc_intervalo,
tipo_interaccion		 											 									ds_cc_tipo_interaccion,
canal					 											 									ds_cc_canal,
NULL 				 											 										ds_cc_asunto,
CASE WHEN trim(last_queue)='NONE' then NULL else trim(last_queue) end   								ds_cc_last_queue,
CASE WHEN trim(last_vqueue)='NONE' then NULL else trim(last_queue) end									ds_cc_last_vqueue,
`from`																 									ds_cc_from,
apellido																								ds_cc_apellido,
nombre																									ds_cc_nombre,
NULL 						 																			cod_per_nup,
NULL 																									ds_cc_numdoc,
NULL 																									ds_cc_cod_disposicion,
motivo_cierre																							ds_cc_motivo_cierre,
from_unixtime(unix_timestamp(inicio_gestion_interaccion,'dd/MM/yyyy HH:mm:ss'),'yyyy-MM-dd HH:mm:ss') 	ts_cc_inicio_gestioninteraccion,
from_unixtime(unix_timestamp(fin_gestion_interaccion,'dd/MM/yyyy HH:mm:ss'),'yyyy-MM-dd HH:mm:ss') 		ts_cc_fin_gestioninteraccion,
agente																									ds_cc_agente,
UPPER(employee_id)																						cod_cc_legajo,
from_unixtime(unix_timestamp(fin_interaccion,'dd MMM yyyy HH:mm:ss'),'yyyy-MM-dd HH:mm:ss') 			ts_cc_fin_interaccion,
from_unixtime(unix_timestamp(inicio_interaccion,'dd MMM yyyy HH:mm:ss'),'yyyy-MM-dd HH:mm:ss') 			ts_cc_inicio_interaccion,
partition_date

from  bi_corp_staging.infomart_detallado_hist hist
Where partition_date= '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_CONTACT_CENTER') }}'
AND '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_CONTACT_CENTER') }}' < '2020-10-01';
