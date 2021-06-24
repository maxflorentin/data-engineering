SET mapred.job.queue.name=root.dataeng;
SET hive.exec.dynamic.partition.mode=nonstrict;

INSERT  overwrite TABLE bi_corp_common.stk_cc_infomartagenteskill PARTITION (partition_date)

select
DISTINCT UPPER(user_name)  			cod_cc_legajo,
trim(first_name)  	ds_cc_nombre,
trim(last_name)   	ds_cc_apellido,
trim(login_code)  	ds_cc_codigo_login,
trim(skill_name)  	ds_cc_skill,
trim(`level`)       	ds_cc_nivel,
partition_date
from bi_corp_staging.infomart_agentes_skill
where partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_CONTACT_CENTER') }}';