SET mapred.job.queue.name=root.dataeng;
SET hive.exec.dynamic.partition.mode=nonstrict;

INSERT OVERWRITE TABLE BI_CORP_COMMON.BT_MYA_APERTURA
PARTITION (partition_date)

select
DISTINCT  case when trim(msg_id)='null'
	 then NULL else trim(msg_id) end				COD_MYA_MENSAJE,
cast(id_subscriber as bigint)						COD_PER_NUP,
case when trim(entitlement)='null'
	 then NULL else trim(entitlement) end			DS_MYA_NOVEDAD,
case when trim(accion)='null'
	 then NULL else trim(accion) end				COD_MYA_ACCION,
case when trim(destination)='null'
	 then NULL else trim(destination) end			COD_MYA_DESTINO,
case when trim(`timestamp`)='null'
	 then NULL else SUBSTRING(`timestamp`,1,19) end	TS_MYA_FECHA,
partition_date

from bi_corp_staging.rio74_links
where partition_date="{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_MYA-Daily') }}";