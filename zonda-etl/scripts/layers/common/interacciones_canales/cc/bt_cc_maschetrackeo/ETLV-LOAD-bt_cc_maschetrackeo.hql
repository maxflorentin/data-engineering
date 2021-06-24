
SET mapred.job.queue.name=root.dataeng;
SET hive.exec.dynamic.partition.mode=nonstrict;

INSERT  overwrite TABLE bi_corp_common.bt_cc_maschetrackeo PARTITION (partition_date)

SELECT
DISTINCT 	interaccion 			    as  cod_cc_interaccion,
	codigo_evento 			    as cod_cc_evento,
	descripcion                 as ds_cc_envento,
	from_unixtime(unix_timestamp(TRIM( SUBSTRING(T.fecha_evento, 0,19 )),'yyyy-MM-dd HH:mm:ss')) as ts_cc_evento,
	CASE WHEN informacion_adicional = 'null' THEN  NULL ELSE TRIM(informacion_adicional) END as ds_cc_infoadicional,
	nro_llamado 			  as cod_cc_nrollamado,
	id_trackeo_transaccion 	as cod_cc_transaccion,
	t.partition_date
FROM
	bi_corp_staging.rio226_tbl_trackeo T
LEFT JOIN bi_corp_staging.rio226_tbl_trackeo_evento E on (E.codigo=T.codigo_evento
		and E.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_CONTACT_CENTER') }}')
 Where
 	T.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_CONTACT_CENTER') }}';
