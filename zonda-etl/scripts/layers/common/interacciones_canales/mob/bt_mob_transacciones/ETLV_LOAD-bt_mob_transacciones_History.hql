"
SET mapred.job.queue.name=root.dataeng;
SET hive.exec.dynamic.partition.mode=nonstrict;

INSERT OVERWRITE TABLE bi_corp_common.bt_mob_transacciones
PARTITION (partition_date)
SELECT
est.id_estadistica 																						id_mob_estadistica,
case when SUBSTRING(est.fecha,1,19)='null' then NULL else SUBSTRING(est.fecha,1,19) end					ts_mob_fecha,
case when trim(est.transaccion)='null' then NULL else trim(est.transaccion) end							cod_mob_transaccion,
trim(tr.descripcion) 																					ds_mob_transaccion,
trim(tr.modulo)																							ds_mob_modulo,
CASE WHEN trim(tr.habilitado)='1' THEN 1 ELSE 0 END														flag_mob_habilitado,
CAST(est.nup_individuo	AS BIGINT)																		cod_per_nup,
CAST(est.nup_empresa	AS BIGINT)																		cod_mob_nup_empresa,
CAST(est.importe_debito AS DECIMAL(19,4))																fc_mob_importe_debito,
CAST(est.importe_credito AS DECIMAL(19,4))																fc_mob_importe_credito,
CASE WHEN est.moneda in ('null','A','M') THEN NULL  ELSE trim(est.moneda)   END							cod_mob_divisa,
CASE WHEN est.ip='null' THEN NULL ELSE TRIM(est.ip) END													ds_mob_ip,
CASE WHEN est.id_terminal='null' THEN NULL ELSE TRIM(est.id_terminal) END 								cod_mob_id_terminal,
CASE WHEN est.token='null' THEN NULL ELSE TRIM(est.token) END 											cod_mob_token,
CASE WHEN dis.id_runtime='null' THEN NULL ELSE TRIM(dis.id_runtime) END  								cod_mob_dispositivo_idruntime,
CASE WHEN dis.device_id='null' THEN NULL ELSE TRIM(dis.device_id) END 									ds_mob_dispositivo_id,
CASE WHEN dis.device_model='null' THEN NULL ELSE TRIM(dis.device_model) END 							ds_mob_dispositivo_modelo,
CASE WHEN dis.fecha='null' THEN NULL ELSE SUBSTRING(TRIM(dis.fecha),1,19) END 							ts_mob_dispositivo_fecha,
CASE WHEN dis.device_hardware_id='null' THEN NULL ELSE SUBSTRING(dis.device_hardware_id,1,10) END 		ds_mob_dispositivo_hw,
CASE WHEN dis.device_mac_address='null' THEN NULL ELSE TRIM(dis.device_mac_address) END 				ds_mob_dispositivo_macadress,
CASE WHEN dis.device_version_so='null' THEN NULL ELSE TRIM(dis.device_version_so) END 					ds_mob_dispositivo_versionSO,
CASE WHEN dis.device_os_id='null' THEN NULL ELSE TRIM(dis.device_os_id) END 							ds_mob_dispositivo_osID,
CASE WHEN est.comprobante='null' THEN NULL ELSE TRIM(est.comprobante) END 								ds_mob_comprobante,
CAST(est.cuenta_origen as BIGINT)																		cod_mob_cuenta_origen,
CAST(est.cuenta_destino as BIGINT)																		cod_mob_cuenta_destion,
CASE WHEN est.nro_solicitud='null' THEN NULL ELSE TRIM(est.nro_solicitud) END 							ds_mob_nro_solicitud,
CASE WHEN est.est_solicitud='null' THEN NULL ELSE TRIM(est.est_solicitud) END 							ds_mob_estado_solicitud,
CASE WHEN est.fecha_inicio_trn='null' THEN NULL ELSE SUBSTRING(est.fecha_inicio_trn,1,19) END 			ts_mob_fecha_iniciotrn,
CASE WHEN est.fecha_fin_trn='null' THEN NULL ELSE SUBSTRING(est.fecha_fin_trn,1,19) END 				ts_mob_fecha_fintrn,
CAST(est.cuit_empresa as BIGINT)																		ds_mob_cuit_empresa,
CAST(est.documento as BIGINT)																			ds_mob_documento,
CAST(est.importe_debito_usd as DECIMAL(19,4))															fc_mob_importe_debitousd,
CASE WHEN est.servidor='null' THEN NULL ELSE TRIM(est.servidor) END 									ds_mob_servidor,
CASE WHEN est.cod_res='null' THEN NULL ELSE TRIM(est.cod_res) END  										cod_mob_res,
CASE WHEN est.latitud='null' THEN NULL ELSE TRIM(est.latitud) END 							 			ds_mob_latitud,
CASE WHEN est.longitud='null' THEN NULL ELSE TRIM(est.longitud) END 									ds_mob_longitud,
CASE WHEN est.version='null' THEN NULL ELSE TRIM(est.version) END 										ds_mob_version,
CASE WHEN est.email_dest='null' THEN NULL ELSE TRIM(est.email_dest) END 								ds_mob_email_dest,
CAST(est.documento_dest AS BIGINT)																		ds_mob_documento_dest,
CASE WHEN est.moneda_debito in ('null','N','T ') THEN NULL  ELSE trim(est.moneda_debito) END 			cod_mob_divisa_debito,
CAST(est.cotizacion	AS DECIMAL(9,4))                                        							ds_mob_cotizacion,
CASE WHEN est.empserv='null' THEN NULL ELSE TRIM(est.empserv) END 										ds_mob_empserv,
est.partition_date																						partition_date

FROM 
(SELECT *
 FROM bi_corp_staging.rio78_estadisticas
 WHERE trim(transaccion) != 'null' and
 partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_MOB_History') }}') est
LEFT JOIN bi_corp_staging.rio78_transaccion tr on (tr.transaccion=est.transaccion and tr.partition_date=IF('{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_MOB_History') }}'< '2020-06-17', '2020-06-17','{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_MOB_History') }}' ))
LEFT JOIN  bi_corp_staging.rio78_dispositivo dis on (dis.id_terminal= est.id_terminal and  dis.partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_MOB_History') }}')

"