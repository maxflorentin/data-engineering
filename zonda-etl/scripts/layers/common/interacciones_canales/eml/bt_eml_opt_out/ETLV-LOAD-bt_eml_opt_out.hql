"
SET mapred.job.queue.name=root.dataeng;
SET hive.exec.dynamic.partition.mode=nonstrict;

INSERT OVERWRITE TABLE bi_corp_common.bt_eml_opt_out
PARTITION (partition_date)

select
DISTINCT event_type_id										cod_eml_evento,
CASE WHEN account_id='null'
	 THEN NULL ELSE TRIM(account_id) END 											cod_eml_cuenta,
CASE WHEN list_id='null'
	 THEN NULL ELSE TRIM(list_id) END 												cod_eml_lista,
CASE WHEN riid='null'
	 THEN NULL ELSE TRIM(riid) END 												cod_eml_riid,
CASE WHEN customer_id='null'
	 THEN NULL ELSE TRIM(customer_id) END 											cod_eml_cliente,
SUBSTRING(event_captured_dt,1,19)					ts_eml_fecha,
SUBSTRING(event_stored_dt ,1,19)					ts_eml_fecha_guardado,
CASE WHEN campaign_id='null'
	 THEN NULL ELSE TRIM(campaign_id) END 			cod_eml_campania,
CASE WHEN launch_id='null'
	 THEN NULL ELSE TRIM(launch_id) END 											cod_eml_lanzamiento,
CASE WHEN email='null'
	 THEN NULL ELSE TRIM(email) END 												ds_eml_email,
CASE WHEN email_format='null'
	 THEN NULL ELSE TRIM(email_format) END 										ds_eml_email_format,
CASE WHEN `source`='null'
	 THEN NULL ELSE TRIM(`source`) END 				ds_eml_origen,
CASE WHEN contact_info='null'
	 THEN NULL ELSE TRIM(contact_info) END 			ds_eml_contacto,
CASE WHEN reason='null'
	 THEN NULL ELSE TRIM(reason) END 				ds_eml_reason,
partition_Date

from bi_corp_staging.responsys_opt_out
where partition_date="{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_EML') }}"
"