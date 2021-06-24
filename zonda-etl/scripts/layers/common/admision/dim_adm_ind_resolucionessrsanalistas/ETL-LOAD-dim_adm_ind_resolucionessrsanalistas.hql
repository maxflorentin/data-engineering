set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=4000000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.exec.dynamic.partition.mode=nonstrict;
SET mapred.job.queue.name=root.dataeng;

INSERT OVERWRITE TABLE bi_corp_common.dim_adm_ind_resolucionessrsanalistas
PARTITION (partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Admision_Ind-Daily') }}')

select
case when trim(legajo) = 'null' then NULL else trim(legajo) end as cod_adm_legajo,
case when trim(usuario_srs) = 'null' then NULL else trim(usuario_srs) end as ds_adm_usuario_srs,
case when trim(usuario) = 'null' then NULL else trim(usuario) end as ds_adm_usuario,
case when trim(rol) = 'null' then NULL else trim(rol) end as ds_adm_rol,
case when trim(equipo) = 'null' then NULL else trim(equipo) end as ds_adm_equipo,
case trim(activo) when 'true' then 1 when 'false' then 0 else Null end as flag_adm_activo,
case when trim(fecha_alta) = 'null' then NULL else trim(fecha_alta) end as ts_adm_fechaalta,
case when trim(fecha_baja) = 'null' then NULL else trim(fecha_baja) end as ts_adm_fechabaja,
case when trim(seniority) = 'null' then NULL else trim(seniority) end as ds_adm_seniority,
case trim(cpi) when 'true' then 1 when 'false' then 0 else Null end as flag_adm_cpi
from bi_corp_staging.risksql5_admision_usuarios
where partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Admision_Ind-Daily') }}'
;



