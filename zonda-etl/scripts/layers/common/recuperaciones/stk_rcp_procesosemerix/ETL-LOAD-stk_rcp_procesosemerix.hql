set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=4000000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.exec.dynamic.partition.mode=nonstrict;
set mapred.job.queue.name=root.dataeng;

insert overwrite table bi_corp_common.stk_rcp_procesosemerix
partition(partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_RECUPERACIONES_CacsEmerix-Daily') }}')

SELECT
cast(num_cliente as bigint) as cod_per_nup,
case when trim(contrato) in ("null","") then NULL else trim(contrato) end as ds_rcp_contratolargo,
case when trim(codigo_estado) = "Sin informar" then "S/I" else trim(codigo_estado) end as cod_rcp_estado,
trim(nombre_estado) as ds_rcp_estado,
case when trim(codigo_escenario) = "Sin informar" then "S/I" else trim(codigo_escenario) end as cod_rcp_escenario,
trim(nombre_escenario) as ds_rcp_escenario,
trim(codigo_agencia) as cod_rcp_buffete,
trim(nombre_agecia) as ds_rcp_nombrebuffete,
concat(trim(nombre_agecia)," ",trim(codigo_agencia)) as ds_rcp_buffete,
trim(numero_proceso) as ds_rcp_numeroproceso,
trim(estado_proceso) as ds_rcp_estadoproceso,
case when substring(trim(fecha_alta_proceso),1,10) in ('9999-12-31', '0001-01-01','null','') then NULL else substring(trim(fecha_alta_proceso),1,10) end as dt_rcp_fechaaltaproceso,
case when substring(trim(fecha_fin_proceso),1,10) in ('9999-12-31', '0001-01-01','null','') then NULL else substring(trim(fecha_fin_proceso),1,10) end as dt_rcp_fechafinproceso,
trim(tipo_proceso) as ds_rcp_tipoproceso,
case when trim(juzgado) in ("null","") then NULL else trim(juzgado) end as ds_rcp_juzgado,
case when trim(secretaria) in ("null","") then NULL else trim(secretaria) end as ds_rcp_secretaria,
case when trim(expediente) in ("null","") then NULL else trim(expediente) end as ds_rcp_expediente,
cast(riesgo_contrato as decimal(17,4)) as fc_rcp_riesgototal,
cast(substring(contrato,9,12) as bigint) as cod_nro_cuenta,
substring(contrato,21,2) as cod_prod_producto,
substring(contrato,23,4) as cod_prod_subproducto,
substring(contrato,27,3) as cod_div_divisa,
cast(substring(contrato,5,4) as int) as cod_suc_sucursal
from bi_corp_staging.emerix_procesos
where partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_RECUPERACIONES_CacsEmerix-Daily') }}'
;