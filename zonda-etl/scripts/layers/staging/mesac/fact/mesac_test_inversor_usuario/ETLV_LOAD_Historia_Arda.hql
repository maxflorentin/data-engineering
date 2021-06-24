"
SET mapred.job.queue.name=root.dataeng;
SET hive.exec.dynamic.partition.mode=nonstrict;


insert overwrite table bi_corp_staging.mesac_test_inversor_usuario
partition(partition_date)
 select
 cod_test id_test,
 num_cliente nup_cliente,
 nup_realiza nup_realiza,
 fec_test fecha_test,
 resultado_test resultado_test,
 cod_perfil id_perfil,
 cod_canal,
 from_unixtime(unix_timestamp(data_date_part,'yyyyMMdd'), 'yyyy-MM-dd') partition_date
 from santander_business_risk_arda.mesac_test_inversor_usuario -- 2019-09-17
 where from_unixtime(unix_timestamp(data_date_part,'yyyyMMdd'), 'yyyy-MM-dd') = "{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_STG_MESAC-Desde_Arda_Catchup') }}"

 "