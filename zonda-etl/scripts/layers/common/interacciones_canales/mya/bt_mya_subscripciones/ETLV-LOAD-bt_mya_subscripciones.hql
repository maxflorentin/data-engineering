"
SET hive.exec.dynamic.partition.mode=nonstrict;
SET mapred.job.queue.name=root.dataeng;

INSERT OVERWRITE TABLE bi_corp_common.bt_mya_subscripciones
PARTITION (partition_date)

select
DISTINCT id_subscription													ID_MYA_SUBSCRIPCION,
id_entitlement 													COD_MYA_NOVEDAD,
cast(id_subscriber as bigint) 									COD_PER_NUP,
id_device 														COD_MYA_DISPOSITIVO,
seqnum															DS_MYA_SECUENCIA_NUM,
id_timeframe													COD_MYA_TIEMPO_ENVIO,
id_status 														COD_MYA_ESTADO,
id_days_to_maturity												DS_MYA_CANT_DIASENVIOS,
id_frequency 													DS_MYA_FRECUENCIA,
case when more_data='null' then NULL else more_data end 		DS_MYA_OBSERVACIONES,
case when one_time='N' then 0 else 1 end 						FLAG_MYA_UNICA_VEZ,
SUBSTRING(create_date,1,19)										TS_MYA_FECHA_CREACION,
SUBSTRING(modify_date,1,19)										TS_MYA_FECHA_MODIFICACION,
case when id_operador='null' then NULL else id_operador end 	DS_MYA_OPERADOR,
partition_date

from bi_corp_staging.rio74_subscription
where partition_Date="{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_MYA-Daily') }}"
"