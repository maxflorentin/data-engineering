"
SET mapred.job.queue.name=root.dataeng;
SET hive.exec.dynamic.partition.mode=nonstrict;

INSERT  overwrite TABLE bi_corp_common.bt_cc_ivropciones
PARTITION (partition_date)

select
DISTINCT substring(fecha,1,19)      													ts_cc_fecha,
sesion					   													cod_cc_sesion,
CASE WHEN opcion_1='null' then NULL else trim(opcion_1)	end				   	ds_cc_ocpion1,
CASE WHEN opcion_2='null' then NULL else trim(opcion_2)	end				   	ds_cc_ocpion2,
CASE WHEN opcion_3='null' then NULL else trim(opcion_3)	end				   	ds_cc_ocpion3,
CASE WHEN opcion_4='null' then NULL else trim(opcion_4)	end				   	ds_cc_ocpion4,
CASE WHEN opcion_5='null' then NULL else trim(opcion_5)	end				   	ds_cc_ocpion5,
partition_date			   													partition_Date
FROM bi_corp_staging.rio32_HISTESTADPREATENDEDOR
WHERE partition_date="{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_CONTACT_CENTER') }}"
"