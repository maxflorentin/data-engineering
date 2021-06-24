set hive.exec.dynamic.partition.mode=nonstrict;
SET mapred.job.queue.name=root.dataeng;

DROP TABLE IF EXISTS cloudera_logs_support_team;

CREATE TEMPORARY TABLE cloudera_logs_support_team AS
SELECT UPPER(cod_legajo) AS cod_legajo
FROM santander_business_risk_arda.nomina_empleados
WHERE data_date_part = '{{ ti.xcom_pull(task_ids='InputConfig', key='max_partition_prevmonth_nomina_empleados', dag_id='CORE_Cloudera_logs_table_classifier_Manual') }}'
AND (
cod_centro_costo = '000-030' OR
cod_centro_costo = '000-038');

CREATE TEMPORARY TABLE cloudera_logs_table_operations AS
SELECT lower(servicevalues.database_name) AS database_name,
lower(servicevalues.table_name)    AS table_name,
TO_DATE(max(`timestamp`))          AS last_access_ts
FROM bi_corp_staging.cloudera_logs cl
LEFT JOIN cloudera_logs_support_team st ON UPPER(split(cl.username, '@')[0]) = st.cod_legajo
WHERE st.cod_legajo IS NULL
AND UPPER(split(cl.username, '@')[0]) IS NOT NULL
AND lower(cl.service) IN ('hive', 'impala')
AND servicevalues.table_name IS NOT NULL
AND lower(servicevalues.database_name) IN ('analytics',
'bi_corp_bdr',
'bi_corp_common',
'bi_corp_business',
'bi_corp_cg',
'bi_corp_ic',
'bi_corp_risk',
'bi_corp_staging',
'santander_bi_bdr',
'santander_business_risk_arda',
'santander_business_risk_ifrs9')
AND TO_DATE((`timestamp`)) between add_months(
    date_add('{{ ti.xcom_pull(task_ids='InputConfig', key='date_from', dag_id='CORE_Cloudera_logs_table_classifier_Manual') }}',1 - day('{{ ti.xcom_pull(task_ids='InputConfig', key='date_from', dag_id='CORE_Cloudera_logs_table_classifier_Manual') }}')),-12) and
    date_add('{{ ti.xcom_pull(task_ids='InputConfig', key='date_from', dag_id='CORE_Cloudera_logs_table_classifier_Manual') }}',1 - day('{{ ti.xcom_pull(task_ids='InputConfig', key='date_from', dag_id='CORE_Cloudera_logs_table_classifier_Manual') }}'))
GROUP BY lower(servicevalues.database_name), lower(servicevalues.table_name);

DROP TABLE IF EXISTS cloudera_logs_results;

CREATE TEMPORARY TABLE cloudera_logs_results AS
SELECT ct.database_name,
ct.table_name,
COALESCE(top.last_access_ts, '1901-01-01') AS last_access_date
FROM bi_corp_staging.cloudera_logs_tables ct
LEFT JOIN cloudera_logs_table_operations top
ON top.database_name = lower(ct.database_name) AND
top.table_name = lower(ct.table_name)
WHERE ct.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='max_partition_prevmonth_cloudera_logs_tables', dag_id='CORE_Cloudera_logs_table_classifier_Manual') }}';

insert overwrite table bi_corp_staging.cloudera_logs_table_classifier
partition(partition_date)
SELECT database_name, table_name, max(last_access_date) AS last_access_date,
CASE
WHEN (months_between('{{ ti.xcom_pull(task_ids='InputConfig', key='previous_month_to', dag_id='CORE_Cloudera_logs_table_classifier_Manual') }}', TO_DATE(max(last_access_date)))) <= 6 THEN "Hot"
WHEN (months_between('{{ ti.xcom_pull(task_ids='InputConfig', key='previous_month_to', dag_id='CORE_Cloudera_logs_table_classifier_Manual') }}', TO_DATE(max(last_access_date)))) <= 12 THEN "Cold"
ELSE "Frozen"
END AS label,
'{{ ti.xcom_pull(task_ids='InputConfig', key='previous_month_partition_bdr', dag_id='CORE_Cloudera_logs_table_classifier_Manual') }}' as partition_date
FROM cloudera_logs_results
WHERE database_name IN ('analytics',
'bi_corp_bdr',
'bi_corp_common',
'bi_corp_business',
'bi_corp_cg',
'bi_corp_ic',
'bi_corp_risk',
'bi_corp_staging',
'santander_bi_bdr',
'santander_business_risk_arda',
'santander_business_risk_ifrs9')
GROUP BY database_name, table_name;


