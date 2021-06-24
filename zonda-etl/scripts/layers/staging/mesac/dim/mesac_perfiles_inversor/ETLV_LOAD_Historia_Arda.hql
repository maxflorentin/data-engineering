"
SET mapred.job.queue.name=root.dataeng;
SET hive.exec.dynamic.partition.mode=nonstrict;


insert overwrite table bi_corp_staging.mesac_perfiles_inversor
partition(partition_date)
 select
 cod_perfil,
 desc_perfil,
 NULL desc_completa,
 from_unixtime(unix_timestamp(data_date_part,'yyyyMMdd'), 'yyyy-MM-dd') partition_date
 from santander_business_risk_arda.mesac_perfiles_inversor
 where from_unixtime(unix_timestamp(data_date_part,'yyyyMMdd'), 'yyyy-MM-dd')  = "{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_STG_MESAC-Desde_Arda_Catchup') }}"

"