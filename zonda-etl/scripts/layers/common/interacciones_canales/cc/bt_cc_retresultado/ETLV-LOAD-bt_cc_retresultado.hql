"
set mapred.job.queue.name=root.dataeng;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;

INSERT OVERWRITE TABLE bi_corp_common.bt_cc_retresultado
PARTITION(partition_date)

SELECT
DISTINCT enc.OPERACION 															cod_cc_operacion,
enc.encuesta															cod_cc_encuesta,
SUBSTRING(from_unixtime(unix_timestamp(enc.fecha,'yyyyMMdd')),1,10)		dt_cc_fecha,
CAST(trim(clavehost) AS BIGINT)											cod_per_nup,
CAST(nuptitular AS BIGINT)												cod_per_nuptitular,
trim(op.usuario)																cod_cc_legajo,
CAST(trim(nrodoc)	 AS BIGINT)							    			ds_cc_numdoc,
CAST(enc.sucursal AS INT)												cod_cc_sucursal,
CAST(nrocuenta AS BIGINT)												cod_cc_cuenta,
CASE WHEN TRIM(op.com_rel)='' THEN NULL ELSE TRIM(op.com_rel) END 		ds_cc_comentario,
CAST(numero_visa AS BIGINT)												cod_cc_nrovisa,
CAST(cuenta_visa  AS BIGINT)											cod_cc_cuentavisa,
CAST(numero_amex AS BIGINT)												cod_cc_nroamex,
CAST(cuenta_amex  AS BIGINT)											cod_cc_cuentaamex,
CAST(numero_master AS BIGINT)											cod_cc_nromaster,
CAST(cuenta_master  AS BIGINT)											cod_cc_cuentamaster,
enc.partition_date														partition_date

FROM bi_corp_staging.rio3_operacionencuesta_rionline enc
LEFT JOIN bi_corp_staging.rio3_operacion op on  (
op.operacion=enc.operacion and  op.partition_date="{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_CONTACT_CENTER') }}")
LEFT JOIN bi_corp_staging.rio3_contacto con on
(op.contacto= con.contacto and con.partition_date="{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_CONTACT_CENTER') }}")
where enc.partition_date="{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_CONTACT_CENTER') }}"

"