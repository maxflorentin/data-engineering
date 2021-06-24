"set mapred.job.queue.name=root.dataeng;
SELECT a.db_id,a.name,a.owner_name,a.tbl_id,a.tbl_name,a.tbl_type,a.column_name,a.type_name,'Campo Nuevo' as comentario,
( case when (a.column_name like '%num%doc%' OR a.column_name like '%nro%doc%' OR a.column_name like '%dni%' OR a.column_name like '%cuil%' OR a.column_name like '%razon_social%' OR a.column_name like '%identper%' OR a.column_name like '%docu%') then ('Documento de identificación') else
( case when (a.column_name like '%num%tar%' OR a.column_name like '%nro%tar%') then ('Número de tarjerta') else
( case when (a.column_name like '%mail%' OR a.column_name like '%cor%elec%') then ('Correo electrónico') else
( case when (a.column_name like '%tel%' and a.column_name not like '%tip%') then ('Teléfono') else
( case when (a.column_name like '%razon%so%') then ('Razón social') else
( case when (a.column_name like '%cuit%' and a.column_name not like '%circuito%' ) then ('CUIT') else
( case when (a.column_name like '%dir%' and (a.column_name not like '%director%' AND a.column_name not like '%dir_oper%')) OR (a.column_name like '%dom%' and (a.column_name not like '%saldom%' AND a.column_name not like '%domain%')) then ('Dirección') else
( case when (a.column_name like '%ape%' and (a.column_name not like '%aper%' AND a.column_name not like '%ogape%' AND a.column_name not like '%preap%' )) then ('Apellido') else
( case when ((a.column_name like '%nom%' OR a.column_name like '%dgrupo%') and (a.column_name not like '%reconom%' AND a.column_name not like '%nomi%' AND a.column_name not like '%impt%nom%')) then ('Nombre') else (
  case when (a.column_name like '%mar%emp%') then 'marcaEmpleado' else
( case when (a.name = 'bi_corp_staging' and a.tbl_name ='malbgc_bgdtmob') then 'Secreto' else 'otro' end )
  end ) end ) end ) end ) end ) end ) end ) end ) end ) end ) as motivo
FROM (
            select db_id,name,owner_name,tbl_id,tbl_name,tbl_type,column_name,type_name
            from bi_corp_staging.hive_metastore_report
            where partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='date_from', dag_id='REPORT_HIVE_METASTORE') }}') as a
LEFT JOIN (
            select db_id,name,owner_name,tbl_id,tbl_name,tbl_type,column_name,type_name
            from bi_corp_staging.hive_metastore_report
            where partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='previous_week_same_date_from', dag_id='REPORT_HIVE_METASTORE') }}')as b
ON (a.tbl_name=b.tbl_name and a.column_name=b.column_name and a.db_id=b.db_id)
where a.name like 'bi_corp_%'and b.tbl_name is null

union all

SELECT a.db_id,a.name,a.owner_name,a.tbl_id,a.tbl_name,a.tbl_type,a.column_name,a.type_name,'Tabla Recreada' as comentario,
( case when (a.column_name like '%num%doc%' OR a.column_name like '%nro%doc%' OR a.column_name like '%dni%' OR a.column_name like '%cuil%' OR a.column_name like '%razon_social%' OR a.column_name like '%identper%' OR a.column_name like '%docu%') then ('Documento de identificación') else
( case when (a.column_name like '%num%tar%' OR a.column_name like '%nro%tar%') then ('Número de tarjerta') else
( case when (a.column_name like '%mail%' OR a.column_name like '%cor%elec%') then ('Correo electrónico') else
( case when (a.column_name like '%tel%' and a.column_name not like '%tip%') then ('Teléfono') else
( case when (a.column_name like '%razon%so%') then ('Razón social') else
( case when (a.column_name like '%cuit%' and a.column_name not like '%circuito%' ) then ('CUIT') else
( case when (a.column_name like '%dir%' and (a.column_name not like '%director%' AND a.column_name not like '%dir_oper%')) OR (a.column_name like '%dom%' and (a.column_name not like '%saldom%' AND a.column_name not like '%domain%')) then ('Dirección') else
( case when (a.column_name like '%ape%' and (a.column_name not like '%aper%' AND a.column_name not like '%ogape%' AND a.column_name not like '%preap%' )) then ('Apellido') else
( case when ((a.column_name like '%nom%' OR a.column_name like '%dgrupo%') and (a.column_name not like '%reconom%' AND a.column_name not like '%nomi%' AND a.column_name not like '%impt%nom%')) then ('Nombre') else (
  case when (a.column_name like '%mar%emp%') then 'marcaEmpleado' else
( case when (a.name = 'bi_corp_staging' and a.tbl_name ='malbgc_bgdtmob') then 'Secreto' else 'otro' end )
  end ) end ) end ) end ) end ) end ) end ) end ) end ) end ) as motivo
FROM (
            select db_id,name,owner_name,tbl_id,tbl_name,tbl_type,column_name,type_name,param_value
            from bi_corp_staging.hive_metastore_report
            where partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='date_from', dag_id='REPORT_HIVE_METASTORE') }}') as a
LEFT JOIN (
            select db_id,name,owner_name,tbl_id,tbl_name,tbl_type,column_name,type_name,param_value
            from bi_corp_staging.hive_metastore_report
            where partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='previous_week_same_date_from', dag_id='REPORT_HIVE_METASTORE') }}')as b
ON (a.tbl_name=b.tbl_name and a.column_name=b.column_name and a.db_id=b.db_id and a.db_id=b.db_id)
where a.name like 'bi_corp_%' and a.tbl_id!=b.tbl_id and b.tbl_name is not null;
"