"
SET hive.exec.dynamic.partition.mode=nonstrict;
SET mapred.job.queue.name=root.dataeng;

INSERT OVERWRITE TABLE bi_corp_common.bt_mya_seguimiento
PARTITION (partition_date)

select
DISTINCT msg_id 														                                    COD_MYA_MENSAJE,
cast(nup as bigint) 										                                    COD_PER_NUP,
CASE WHEN trim(link)='null' THEN NULL ELSE trim(link) END 					                        DS_MYA_LINK,
CASE WHEN trim(entitlement)='null' THEN NULL ELSE trim(entitlement)	END			                    DS_MYA_NOVEDAD,
CASE WHEN trim(destination) ='null' THEN NULL ELSE	trim(destination) END			                COD_MYA_DESTINO,
CASE WHEN fecha_envio='null' THEN NULL ELSE SUBSTRING(fecha_envio,1,19) 	END							TS_MYA_FECHA_ENVIO,
SUBSTRING(fecha_log,1,19) 									                                    TS_MYA_FECHA_LOG,
case when cond1='null' then NULL else cond1 end 			                                    DS_MYA_CONDICION1,
case when cond2='null' then NULL else cond2 end 			                                    DS_MYA_CONDICION2,
case when cond3='null' then NULL else cond3 end 			                                    DS_MYA_CONDICION3,
case when cond4='null' then NULL else cond4 end 			                                    DS_MYA_CONDICION4,
partition_date												                                    PARTITION_DATE


from bi_corp_staging.rio74_mya_seguimientos
where partition_Date="{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_MYA-Daily') }}"

"