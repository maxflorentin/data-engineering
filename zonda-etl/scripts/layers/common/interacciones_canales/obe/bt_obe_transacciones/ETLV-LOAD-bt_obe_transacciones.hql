"
SET hive.exec.dynamic.partition.mode=nonstrict;
SET mapred.job.queue.name=root.dataeng;

INSERT OVERWRITE TABLE bi_corp_common.bt_obe_transacciones
PARTITION (partition_date)

select
DISTINCT SUBSTRING(fecha,1,19)																                ts_obe_fecha,
CASE WHEN cuit='null' THEN NULL ELSE CAST(cuit AS BIGINT)	END						                ds_obe_cuit,
CASE WHEN est.transaccion='null' THEN NULL ELSE TRIM(est.transaccion)	END					        cod_obe_transaccion,
CASE WHEN trx.descripcion='null' THEN NULL ELSE	 TRIM(trx.descripcion)	END			                ds_obe_transaccion,
CASE WHEN trx.modulo='null' THEN NULL ELSE	 TRIM(trx.modulo)	END			                        cod_obe_modulo,
CASE WHEN modu.descripcion='null' THEN NULL ELSE TRIM(modu.descripcion)	END				            ds_obe_modulo,
CASE WHEN trx.tipo_trx='null' THEN NULL ELSE	 TRIM(trx.tipo_trx)	END			                    cod_obe_tipo_transaccion,
CASE WHEN tipo.descripcion_tipo='null' THEN NULL ELSE	 TRIM(tipo.descripcion_tipo)	END			 ds_obe_tipo_transaccion,
CASE WHEN trx.submodulo='null' THEN NULL ELSE	 TRIM(trx.submodulo)	END			                cod_obe_submodulo,
CASE WHEN sub.descripcion='null' THEN NULL ELSE	 TRIM(sub.descripcion)	END			                ds_obe_submodulo,
CASE WHEN cod_error='null' THEN NULL ELSE TRIM(cod_error)	END					                    cod_obe_error,
CASE WHEN pid='null' THEN NULL ELSE TRIM(pid)	END								                    ds_obe_pid,
CASE WHEN comprobante='null' THEN NULL ELSE TRIM(comprobante)	END					                ds_obe_comprobante,
CASE WHEN importe='null' THEN NULL ELSE CAST(importe AS decimal(19,4))	END			                fc_obe_importe,
CASE WHEN moneda='null' THEN NULL ELSE
     CASE WHEN (moneda)='$' THEN 'ARS' ELSE moneda	END	END				                            ds_obe_moneda,
CASE WHEN cod_sucursal='null' THEN NULL ELSE CAST(cod_sucursal as INT)	END					        cod_obe_sucursal,
CASE WHEN estado='null' THEN NULL ELSE TRIM(estado)	END								                cod_obe_estado,
CASE WHEN id_solicitud='null' THEN NULL ELSE TRIM(id_solicitud)	END					                cod_obe_solicitud,
CASE WHEN nro_cuenta='null' THEN NULL ELSE TRIM(nro_cuenta)	END						                ds_obe_nro_cuenta,
CASE WHEN tipo_cuenta_crd='null' THEN NULL ELSE TRIM(tipo_cuenta_crd)	END			                ds_obe_tipo_cuentacrd,
CASE WHEN cod_sucursal_crd='null' THEN NULL ELSE CAST(cod_sucursal_crd as INT)	END			        cod_obe_sucursal_crd,
CASE WHEN f_alta_registro='null' THEN NULL ELSE SUBSTRING(f_alta_registro,1,19)	END	                ts_obe_fecha_alta,
CASE WHEN tipo_empresa='null' THEN NULL ELSE TRIM(tipo_empresa)	END					                ds_obe_tipo_empresa,
CASE WHEN destinatario='null' THEN NULL ELSE TRIM(destinatario)	END					                ds_obe_destinatario,
CASE WHEN tipo_teclado='null' THEN NULL ELSE TRIM(tipo_teclado)	END					                ds_obe_tipo_teclado,
CASE WHEN nup='null' THEN NULL ELSE CAST(nup as bigint)	END									        cod_per_nup,
CASE WHEN rpi_medio_pago='null' THEN NULL ELSE TRIM(rpi_medio_pago)	END					            ds_obe_rpi_mediopago,
CASE WHEN rpi_cuitempsev='null' THEN NULL ELSE CAST(rpi_cuitempsev as bigint)	END					ds_obe_rpi_cuitempsev,
CASE WHEN rpi_id_servicio='null' THEN NULL ELSE TRIM(rpi_id_servicio)	END					        ds_obe_rpi_servicio,
CASE WHEN rpi_nomfantasia='null' THEN NULL ELSE TRIM(rpi_nomfantasia)	END					        ds_obe_rpi_nomfantasia,
CASE WHEN rpi_identclienteemp='null' THEN NULL ELSE TRIM(rpi_identclienteemp)	END			        ds_obe_rpi_identclienteemp,
est.partition_date

from bi_corp_staging.rio15_nb_tl_estadisticas est
left join bi_corp_staging.rio15_nb_desc_trx trx on (est.transaccion=trx.transaccion and trx.partition_date="{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_OBE-Daily') }}")
left join bi_corp_staging.rio15_nb_modulos modu on (modu.modulo=trx.modulo and modu.partition_date="{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_OBE-Daily') }}")
left join bi_corp_staging.rio15_nb_submodulos sub  on (sub.submodulo=trx.submodulo and sub.partition_date="{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_OBE-Daily') }}")
left join bi_corp_staging.rio15_nb_tipo_trx tipo on  (tipo.tipo_trx=trx.tipo_trx and tipo.partition_date="{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_OBE-Daily') }}")
where est.partition_date="{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_OBE-Daily') }}"
"
