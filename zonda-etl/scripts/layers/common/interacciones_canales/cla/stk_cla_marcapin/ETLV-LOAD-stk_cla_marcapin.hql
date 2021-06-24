SET mapred.job.queue.name=root.dataeng;
SET hive.exec.dynamic.partition.mode=nonstrict;

INSERT OVERWRITE TABLE bi_corp_common.stk_cla_marcapin
PARTITION (partition_date)

select
DISTINCT cast(nup as bigint)											cod_per_nup,
tipo_clave													cod_cla_tipoclave,
case when trim(fecha_creacion) = ''
	 then null else trim(fecha_creacion) end 				dt_cla_fecha_alta,
case when trim(fecha_cambio_clave) = ''
	 then null else trim(fecha_cambio_clave) end 			dt_cla_fecha_cambio,
case when trim(fecha_ultimo_acceso) = ''
	 then null else trim(fecha_ultimo_acceso) end 			dt_cla_fecha_acceso,
case when trim(marca_bloqueo) ='Y'
	 then 1 else 0 end 													flag_cla_marca_bloqueo,
partition_date

from bi_corp_staging.asgi_marc_pin
where partition_date="{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_CLAVE_SR') }}"
and "{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_CLAVE_SR') }}" >= '2020-11-15'

UNION ALL

select
cast(penumper as bigint)									cod_per_nup,
case when trim(tipo_clave)='null'
	 then NULL else trim(tipo_clave) end 					cod_cla_tipoclave,
case when trim(fecha_alta)='null'
	 then NULL else SUBSTRING(fecha_alta,1,10) end 			dt_cla_fecha_alta,
case when trim(fecha_cambio)='null'
	 then NULL else SUBSTRING(fecha_cambio,1,10) end 		dt_cla_fecha_cambio,
case when trim(fecha_acceso)='null'
	 then NULL else SUBSTRING(fecha_acceso,1,10) end 		dt_cla_fecha_acceso,
case when trim(marca_bloqueo)='Y'
	 then 1 else 0 end 										flag_cla_marca_bloqueo,
partition_date
from bi_corp_staging.rio53_marc_pin
where partition_date="{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_CLAVE_SR') }}"
and "{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_CLAVE_SR') }}" < '2020-11-15';