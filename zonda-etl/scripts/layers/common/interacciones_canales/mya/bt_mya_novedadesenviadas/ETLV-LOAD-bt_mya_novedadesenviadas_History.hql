"
SET hive.exec.dynamic.partition.mode=nonstrict;
SET mapred.job.queue.name=root.dataeng;

INSERT OVERWRITE TABLE bi_corp_common.bt_mya_novedadesenviadas
PARTITION (partition_date)

select
lo.msg_id 														             	    COD_MYA_MENSAJE,
req.msg_req_id 															 		    COD_MYA_MENSAJE_REQ,
lo.id_entitlement														 		    COD_MYA_NOVEDAD,
ent.entitlement															 		    DS_MYA_NOVEDAD,
SUBSTRING(create_timestamp,1,19)           										 	TS_MYA_FECHA_CREACION,
CASE WHEN req.manual='0' THEN 0 ELSE 1 END									 		FLAG_MYA_MANUAL,
CASE WHEN req.processed='1' THEN 1 ELSE 0 END								 		FLAG_MYA_PROCESADO,
TRIM(`req`.`data`) 														 			DS_MYA_MENSAJE,
SUBSTRING(req.timestamp_inicio_procesamiento,1,19)    								TS_MYA_INICIO_PROCESAMIENTO,
CAST(lo.nup AS BIGINT)														 		COD_PER_NUP,
CAST(lo.id_cliente AS BIGINT)												 		DS_MYA_DOCUMENTO,
SUBSTRING(lo.fecha,1,19)                         									TS_MYA_FECHA_LOG,
CASE WHEN lo.destination='null' THEN NULL ELSE trim(lo.destination) END 		 	COD_MYA_DESTINO,
CASE WHEN lo.id_estado='null' THEN NULL ELSE trim(lo.id_estado) END       		    COD_MYA_ESTADO,
CASE WHEN sta.descripcion='null' THEN NULL ELSE trim(sta.descripcion) END 		    DS_MYA_ESTADO,
CASE WHEN lo.id_device='null' THEN NULL ELSE trim(lo.id_device) END 				COD_MYA_DISPOSITIVO,
CASE WHEN lo.id_notificacion='null' THEN NULL ELSE trim(lo.id_notificacion) END 	COD_MYA_NOTIFICACION,
lo.partition_date																	PARTITION_DATE

from bi_corp_staging.rio74_tmya_message_log lo
left join bi_corp_staging.rio74_tmya_message me on (me.msg_id=lo.msg_id and me.partition_Date="{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_MYA-History') }}")
left join  bi_corp_staging.rio74_tmya_message_request_data req on (req.msg_req_id=me.msg_req_id and
        req.partition_date BETWEEN "{{ ti.xcom_pull(task_ids='InputConfig', key='previous_date_from', dag_id='LOAD_CMN_MYA-History') }}" AND
                                    "{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_MYA-History') }}")
left join  bi_corp_staging.rio74_entitlement ent on (ent.id_entitlement=lo.id_entitlement and
ent.partition_date=IF("{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_MYA-History') }}" < "2020-08-03","2020-08-03","{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_MYA-History') }}"))
left join bi_corp_staging.rio74_tmya_status_log sta on ( sta.id_estado=lo.id_estado and
sta.partition_date=IF("{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_MYA-History') }}" < "2020-08-03","2020-08-03","{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_MYA-History') }}"))
where lo.partition_date="{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_MYA-History') }}"

UNION ALL

select
lo_cd.msg_id 														             	    COD_MYA_MENSAJE,
req_cd.msg_req_id 															 		    COD_MYA_MENSAJE_REQ,
lo_cd.id_entitlement														 		    COD_MYA_NOVEDAD,
ent.entitlement															 			    DS_MYA_NOVEDAD,
SUBSTRING(create_timestamp,1,19) 										 				TS_MYA_FECHA_CREACION,
CASE WHEN req_cd.manual='0' THEN 0 ELSE 1 END									 		FLAG_MYA_MANUAL,
CASE WHEN req_cd.processed='1' THEN 1 ELSE 0 END								 		FLAG_MYA_PROCESADO,
TRIM(`req_cd`.`data`) 														 			DS_MYA_MENSAJE,
SUBSTRING(req_cd.timestamp_inicio_procesamiento,1,19)  									TS_MYA_INICIO_PROCESAMIENTO,
CAST(lo_cd.nup AS BIGINT)														 		COD_PER_NUP,
CAST(lo_cd.idcliente AS BIGINT)												 		    DS_MYA_DOCUMENTO,
SUBSTRING(lo_cd.fecha,1,19) 												 		    TS_MYA_FECHA_LOG,
CASE WHEN lo_cd.destination='null' THEN NULL ELSE trim(lo_cd.destination) END 		    COD_MYA_DESTINO,
CASE WHEN lo_cd.id_estado='null' THEN NULL ELSE trim(lo_cd.id_estado) END       		COD_MYA_ESTADO,
CASE WHEN sta.descripcion='null' THEN NULL ELSE trim(sta.descripcion) END 		        DS_MYA_ESTADO,
CASE WHEN lo_cd.id_device='null' THEN NULL ELSE trim(lo_cd.id_device) END 				COD_MYA_DISPOSITIVO,
NULL																			        COD_MYA_NOTIFICACION,
lo_cd.partition_date																	PARTITION_DATE
from bi_corp_staging.rio74_tmya_message_log_cd lo_cd
left join bi_corp_staging.rio74_tmya_message_cd me_cd on (me_cd.msg_id=lo_cd.msg_id and me_cd.partition_Date="{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_MYA-History') }}")
left join  bi_corp_staging.rio74_tmya_message_request_data_cd req_cd on (req_cd.msg_req_id=me_cd.msg_req_id and req_cd.partition_date="{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_MYA-History') }}")
left join  bi_corp_staging.rio74_entitlement ent on (ent.id_entitlement=lo_cd.id_entitlement and
ent.partition_date=IF("{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_MYA-History') }}" < "2020-08-03","2020-08-03","{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_MYA-History') }}"))
left join bi_corp_staging.rio74_tmya_status_log sta on ( sta.id_estado=lo_cd.id_estado and
sta.partition_date=IF("{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_MYA-History') }}" < "2020-08-03","2020-08-03","{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_MYA-History') }}"))
where lo_cd.partition_date="{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_MYA-History') }}"

"