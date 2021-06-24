set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=4000000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.exec.dynamic.partition.mode=nonstrict;
set mapred.job.queue.name=root.dataeng;

INSERT OVERWRITE TABLE bi_corp_common.dim_adm_ind_promociones
PARTITION (partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Admision_Ind-Daily') }}')

select
case when trim(promocion) = 'null' then NULL else trim(promocion) end as cod_adm_promocion,
case when trim(descripcion) = 'null' then NULL else trim(descripcion) end as ds_adm_descripcion,
case when trim(grupo) = 'null' then NULL else trim(grupo) end as ds_adm_grupo,
case trim(activa) when 'true' then 1 when 'false' then 0 else Null end as flag_adm_activa,
case when trim(fecha_alta) = 'null' then NULL else trim(fecha_alta) end as ts_adm_fecha_alta,
case when trim(fecha_baja) = 'null' then NULL else trim(fecha_baja) end as ts_adm_fecha_baja,
case when trim(descripcion2) = 'null' then NULL else trim(descripcion2) end as ds_adm_descripcion2,
case when trim(tramite) = 'null' then NULL else trim(tramite) end as ds_adm_tramite
from bi_corp_staging.risksql5_admision_promociones
where partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Admision_Ind-Daily') }}'
and trim(promocion) != ''
;