"
SET mapred.job.queue.name=root.dataeng;
SET hive.exec.dynamic.partition.mode=nonstrict;

INSERT OVERWRITE TABLE bi_corp_common.bt_mya_rebote
PARTITION (partition_date)

select
id_mail                         id_mya_rebote,
e_mail 							cod_mya_destino,
SUBSTRING(date_reject,1,19) 	ts_mya_fecha,
rea.id_reason					cod_mya_razon,
rea.desc_reason					ds_mya_razon,
tipo.id_type					cod_mya_tipo,
tipo.desc_type				    ds_mya_tipo,
mail.partition_date             partition_date

from bi_corp_staging.rio74_pmr_mail mail
left join bi_corp_staging.rio74_pmr_reason rea on
(mail.id_reason= rea.id_reason and rea.partition_date= IF("{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_MYA-History') }}" < '2020-08-26','2020-08-26',"{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_MYA-History') }}" ))
left join bi_corp_staging.rio74_pmr_type tipo on (rea.id_type= tipo.id_type and
 tipo.partition_date= IF("{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_MYA-History') }}"< '2020-10-01','2020-10-01',"{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_MYA-History') }}"))
where mail.partition_date="{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_MYA-History') }}"

"