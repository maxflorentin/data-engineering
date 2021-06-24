set mapred.job.queue.name=root.dataeng;

--------------Inserto los datos en la dimension de canales

INSERT OVERWRITE TABLE bi_corp_common.dim_cue_canales
select trim(substr(gen_clave,2,10)) cod_cue_canal,
	   upper(gen_datos) ds_cue_canal,
	   CASE WHEN trim(substr(gen_clave,2,10)) ='00' THEN 'SELLSTATION'
	  	   WHEN trim(substr(gen_clave,2,10)) ='01' THEN 'SUPERLINEA AUTOMATICO'
	  	   WHEN trim(substr(gen_clave,2,10)) ='07' THEN 'SUPERLINEA HUMANO'
	  	   WHEN trim(substr(gen_clave,2,10)) ='70' THEN 'APP MOBILE'
	  	   WHEN trim(substr(gen_clave,2,10)) ='71' THEN 'APP MOBILE PYME'
	  	   WHEN trim(substr(gen_clave,2,10)) ='72' THEN 'APP MOBILE CORPORATE'
	  	   WHEN trim(substr(gen_clave,2,10)) ='IN' THEN 'INMOBILIARIAS'
	  	   WHEN trim(substr(gen_clave,2,10)) ='LP' THEN 'LOCALES PROPIOS'
	  	   WHEN trim(substr(gen_clave,2,10)) ='OE' THEN 'ONLINE BANKING EMPRESAS'
	  	   WHEN trim(substr(gen_clave,2,10)) ='PV' THEN 'PUNTO DE VENTA'
	  	   WHEN trim(substr(gen_clave,2,10)) ='WW' THEN 'PORTAL'
	  	   ELSE upper(gen_datos)
	  END AS ds_cue_canal_new
from bi_corp_staging.tcdtgen a
where gentabla = '0347'
and a.partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='max_partition_tcdtgen', dag_id='LOAD_CMN_Cuentas_Hist') }}'