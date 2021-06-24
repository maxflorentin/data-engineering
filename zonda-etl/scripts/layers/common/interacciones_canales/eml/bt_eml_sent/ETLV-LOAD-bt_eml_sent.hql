"
SET mapred.job.queue.name=root.dataeng;
SET hive.exec.dynamic.partition.mode=nonstrict;

INSERT OVERWRITE TABLE bi_corp_common.bt_eml_sent
PARTITION (partition_date)

select
DISTINCT event_type_id                                      				cod_eml_evento ,
CASE WHEN account_id='null'
THEN NULL ELSE trim(account_id) end 														cod_eml_cuenta,
CASE WHEN list_id='null'
THEN NULL ELSE trim(list_id) end  														cod_eml_lista,
CASE WHEN riid='null'
THEN NULL ELSE trim(riid) end  															cod_eml_riid,
CASE WHEN customer_id='null'
THEN NULL ELSE trim(customer_id) end 														cod_eml_cliente,
CASE WHEN event_captured_dt='null'
THEN NULL ELSE trim(event_captured_dt) end                                    			ts_eml_fecha,
CASE WHEN event_stored_dt='null'
THEN NULL ELSE trim(event_stored_dt) end 													ts_eml_fecha_guardado,
CASE WHEN campaign_id='null'
THEN NULL ELSE trim(campaign_id) end 														cod_eml_campania,
CASE WHEN launch_id='null'
THEN NULL ELSE trim(launch_id) end 														cod_eml_lanzamiento,
CASE WHEN email='null'
THEN NULL ELSE trim(email) end 															ds_eml_email,
CASE WHEN email_isp='null'
THEN NULL ELSE trim(email_isp) end 														ds_eml_email_isp,
CASE WHEN email_format='null'
THEN NULL ELSE trim(email_format) end 													ds_eml_email_format,
CASE WHEN offer_signature_id='null'
THEN NULL ELSE trim(offer_signature_id) end 												ds_eml_offer_signature,
CASE WHEN dynamic_content_signature_id='null'
THEN NULL ELSE trim(dynamic_content_signature_id) end			ds_eml_dynamic_contentsignature,
CASE WHEN message_size='null'
THEN NULL ELSE trim(message_size) end 													ds_eml_tamanio_msg,
CASE WHEN segment_info='null'
THEN NULL ELSE trim(segment_info) end													ds_eml_segmento,
CASE WHEN contact_info='null'
THEN NULL ELSE trim(contact_info) end   													ds_eml_contacto,
partition_date
from bi_corp_staging.responsys_sent
where partition_date="{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_EML') }}"

"