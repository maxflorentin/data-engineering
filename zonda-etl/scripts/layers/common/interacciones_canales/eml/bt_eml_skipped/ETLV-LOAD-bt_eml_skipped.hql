"
SET mapred.job.queue.name=root.dataeng;
SET hive.exec.dynamic.partition.mode=nonstrict;

INSERT OVERWRITE TABLE bi_corp_common.bt_eml_skipped
PARTITION (partition_date)

select
DISTINCT event_type_id										cod_eml_evento,
account_id											cod_eml_cuenta,
list_id												cod_eml_lista,
riid												cod_eml_riid,
customer_id											cod_eml_cliente,
SUBSTRING(event_captured_dt,1,19)					ts_eml_fecha,
SUBSTRING(event_stored_dt ,1,19)					ts_eml_fecha_guardado,
campaign_id											cod_eml_campania,
launch_id											cod_eml_lanzamiento,
email												ds_eml_email,
email_isp											ds_eml_email_isp,
email_format										ds_eml_email_format,
CASE WHEN offer_signature_id='null'
	 THEN NULL ELSE TRIM(offer_signature_id) END 			ds_eml_offer_signature,
CASE WHEN dynamic_content_signature_id='null'
	 THEN NULL ELSE TRIM(dynamic_content_signature_id) END 	ds_eml_dynamic_contentsignature,
CASE WHEN message_size='null'
	 THEN NULL ELSE TRIM(message_size) END 					ds_eml_tamanio_msg,
CASE WHEN segment_info='null'
	 THEN NULL ELSE TRIM(segment_info) END 					ds_eml_segmento,
CASE WHEN contact_info='null'
	 THEN NULL ELSE TRIM(contact_info) END 					ds_eml_contacto,
CASE WHEN reason='null'
	 THEN NULL ELSE TRIM(reason) END 					ds_eml_reason,
partition_Date

from bi_corp_staging.responsys_skipped
where partition_date="{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_EML') }}"

"