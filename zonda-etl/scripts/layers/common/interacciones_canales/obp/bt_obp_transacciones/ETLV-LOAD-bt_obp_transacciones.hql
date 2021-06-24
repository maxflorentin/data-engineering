"
SET hive.exec.dynamic.partition.mode=nonstrict;
SET mapred.job.queue.name=root.dataeng;

INSERT OVERWRITE TABLE bi_corp_common.bt_obp_transacciones
PARTITION (partition_date)

select
DISTINCT tran.transaccion		 																	cod_obp_transaccion ,
CASE WHEN trim(descrip.descripcion)='null' THEN NULL ELSE trim(descrip.descripcion) END	 	ds_obp_transaccion,
CASE WHEN trim(tran.tipo_teclado)='null' THEN NULL ELSE trim(tran.tipo_teclado) END			ds_obp_tipo_teclado,
CASE WHEN trim(tran.tipo)='null' THEN NULL ELSE trim(tran.tipo) END						    ds_obp_tipo,
CASE WHEN trim(tran.procesado)='null' THEN NULL ELSE trim(tran.procesado) END				ds_obp_procesado,
CASE WHEN trim(tran.pid)='null' THEN NULL ELSE trim(tran.pid) END							ds_obp_pid,
CAST(tran.nup as BIGINT)																	cod_per_nup,
CASE WHEN trim(tran.num_log)='null' THEN NULL ELSE trim(tran.num_log) END				    ds_obp_num_log,
CASE WHEN trim(tran.nom_server)='null' THEN NULL ELSE trim(tran.nom_server) END			    ds_obp_nom_server,
CASE WHEN tran.moneda='null' THEN NULL
     WHEN trim(tran.moneda)='u$s' THEN 'USD'
     WHEN trim(tran.moneda)='$' THEN 'ARS'
     ELSE trim(tran.moneda)   END							                                cod_obp_divisa,
case when trim(tran.ip)='null' THEN NULL else trim(tran.ip) end							ds_obp_ip,
cast(tran.importe as DECIMAL(19,4))															fc_obp_importe,
CASE WHEN tran.f_alta_registro='null' THEN NULL ELSE SUBSTRING(tran.f_alta_registro,1,19)	end 	ts_obp_fecha_alta,
CASE WHEN tran.fecha='null' THEN NULL ELSE SUBSTRING(tran.fecha,1,19)	end 				ts_obp_fecha,
CAST(tran.documento AS BIGINT)																ds_obp_documento,
case when trim(dispositivo)='null' THEN NULL else trim(dispositivo) end						ds_obp_dispositivo,
cast(tran.cuenta_origen	as 	BIGINT)															cod_obp_cuenta_origen,
cast(tran.cuenta_destino	as BIGINT)														cod_obp_cuenta_destino,
CASE WHEN trim(tran.comprobante)= 'null' THEN NULL ELSE trim(tran.comprobante) END						ds_obp_comprobante,
CASE WHEN trim(tran.cod_error)= 'null' THEN NULL ELSE trim(tran.cod_error) END				ds_obp_error,
CASE WHEN tran.canal_acceso	= 'null' THEN NULL ELSE trim(tran.canal_acceso) END				cod_obp_canal_acceso,
tran.partition_date  																		partition_date

from bi_corp_staging.rio147_transaction_log tran
left join bi_corp_staging.rio147_hb_desc_trx descrip on (descrip.transaccion= tran.transaccion and descrip.partition_Date=tran.partition_Date)
where tran.partition_date="{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_OBP-Daily') }}"



"