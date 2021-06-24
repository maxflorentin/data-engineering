SET mapred.job.queue.name=root.dataeng;
SET hive.exec.dynamic.partition.mode=nonstrict;

INSERT OVERWRITE TABLE bi_corp_common.bt_cla_altareset
PARTITION (partition_date)

select
DISTINCT trim(funcion)  					cod_cla_tipo_operacion,
trim(autorizacion_1)			cod_cla_usuario1,
trim(autorizacion_2)			cod_cla_usuario2,
cast(nup as bigint)				cod_per_nup,
CONCAT(from_unixtime(unix_timestamp(fecha,'yyyyMMdd'),'yyyy-MM-dd'),' ',hora)  ts_cla_fecha,
trim(retorno)							cod_cla_retorno,
partition_Date

from bi_corp_staging.asgi_alta_reset
where partition_date="{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_CLAVE_SR') }}"
AND from_unixtime(unix_timestamp(fecha,'yyyyMMdd'),'yyyy-MM-dd')= "{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_CLAVE_SR') }}"
AND "{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_CLAVE_SR') }}" >= '2020-12-02'

UNION ALL

select
CASE WHEN trim(tipo_operacion)='null'
	 THEN NULL ELSE trim(tipo_operacion) END 				cod_cla_tipo_operacion,
CASE WHEN trim(usuario1)='null'
	 THEN NULL ELSE trim(usuario1) END 						cod_cla_usuario1,
CASE WHEN trim(usuario2)='null'
	 THEN NULL ELSE trim(usuario2) END						cod_cla_usuario2,
cast(nup as bigint)				cod_per_nup,
CONCAT(SUBSTRING(fecha_modificacion,1,10),' ',fecha_hora_modificacion)  ts_cla_fecha,
CASE WHEN trim(cod_retorno)='null'
	 THEN NULL ELSE trim(cod_retorno) END							cod_cla_retorno,
partition_Date


from bi_corp_staging.rio143_alta_reset_hist
where partition_date="{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_CLAVE_SR') }}"
AND "{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_CLAVE_SR') }}" < '2020-12-02';
