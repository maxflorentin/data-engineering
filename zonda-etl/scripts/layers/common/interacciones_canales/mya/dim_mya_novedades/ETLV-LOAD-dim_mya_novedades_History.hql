"
SET mapred.job.queue.name=root.dataeng;
SET hive.exec.dynamic.partition.mode=nonstrict;

INSERT OVERWRITE TABLE bi_corp_common.dim_mya_novedades
PARTITION (partition_date)

select
id_entitlement																			cod_mya_novedad,
case when entitlement='null' then null else  trim(entitlement) end 			            ds_mya_novedad,
case when description='null' then null else  trim(description) end		        		ds_mya_descripcion,
case when long_description='null' then null else  trim(long_description) end 			ds_mya_descripcion_larga,
case when label ='null' then null else  trim(label) end									ds_mya_label,
case when default_frequency ='null' then null else  trim(default_frequency) end			ds_mya_frecuencia,
case when default_timeframe ='null' then null else  trim(default_timeframe) end			ds_mya_periodo_tiempo,
cast(trim(default_days_to_maturity) as int) 											int_mya_cant_diasenvios ,
e.id_status																				cod_mya_estado,
case when visible='s' then 1 else 0 end 												flag_mya_visible_front,
case when visible_search='s' then 1 else 0 end 											flag_mya_visible_busqueda,
case when trim(description_search)='null' then null else  trim(description_search) end 	ds_mya_descripcion_busqueda,
case when priority='null' then null else priority end 									ds_mya_prioridad,
case when id_subchannelbank_default='null' then null else id_subchannelbank_default end cod_mya_subcanal,
case when id_channelbank_default='null' then null else id_channelbank_default end 		cod_mya_canal,
case when id_rule='null' then null else id_rule end 									cod_mya_regla,
case when id_provider='null' then null else id_provider end 							cod_mya_modo_procesamiento,
cast(ttl as int) 																		int_mya_ttl,
case when page_show='null' then null else page_show end 								ds_mya_pagina_muestra,
case when newsletter='0' then 0 else 1 end 												flag_mya_newsletter,
case when id_filter='null' then null else id_filter end 								cod_mya_filtro,
case when tipo_especial='null' then null else tipo_especial end 						ds_mya_especial,
case when acnl='s' then 1 else 0 end 													flag_mya_acnl,
case when url='null' then null else url end 											ds_mya_url,
e.partition_date																		partition_date

from bi_corp_staging.rio74_entitlement e
where e.partition_date= IF("{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_MYA-History') }}" < '2020-08-03','2020-08-03',"{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_MYA-History') }}")

"