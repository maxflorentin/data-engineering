set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=4000000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.exec.dynamic.partition.mode=nonstrict;
SET mapred.job.queue.name=root.dataeng;

INSERT OVERWRITE TABLE bi_corp_common.dim_adm_ind_canales
PARTITION (partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Admision_Ind-Daily') }}')

select
case when trim(codigo) = 'null' then NULL else trim(codigo) end as cod_adm_canal,
case when trim(descripcion) = 'null' then NULL else trim(descripcion) end  as ds_adm_descripcion,
case when trim(agrupador) = 'null' then NULL else trim(agrupador) end  as ds_adm_agrupador,
case when trim(tipo) = 'null' then NULL else trim(tipo) end  as ds_adm_tipo,
case trim(activo) when 'true' then 1 when 'false' then 0 else Null end as flag_adm_activo,
case when trim(fecha_alta) = 'null' then NULL else trim(fecha_alta) end  as ts_adm_fechaalta,
case when trim(fecha_baja) = 'null' then NULL else trim(fecha_baja) end  as ts_adm_fechabaja
from bi_corp_staging.risksql5_admision_canales_de_venta
where partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Admision_Ind-Daily') }}'
;