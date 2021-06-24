SET mapred.job.queue.name=root.dataeng;
SET hive.exec.dynamic.partition.mode=nonstrict;

INSERT OVERWRITE TABLE bi_corp_common.bt_eml_click
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
CASE WHEN email_format='null'
	 THEN NULL ELSE TRIM(email_format) END 										ds_eml_email_format,
CASE WHEN offer_name='null'
	 THEN NULL ELSE TRIM(offer_name) END 										ds_eml_oferta,
CASE WHEN offer_number='null'
	 THEN NULL ELSE TRIM(offer_number) END 										ds_eml_numero_oferta,
CASE WHEN offer_category='null'
	 THEN NULL ELSE TRIM(offer_category) END 										ds_eml_categoria_oferta,
CASE WHEN offer_url='null'
	 THEN NULL ELSE TRIM(offer_url) END 										ds_eml_oferta_url,
partition_Date

from bi_corp_staging.responsys_click
where partition_date="{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_EML') }}";


