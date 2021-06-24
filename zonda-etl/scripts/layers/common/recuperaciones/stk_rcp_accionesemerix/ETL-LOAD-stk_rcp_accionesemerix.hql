set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=4000000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.exec.dynamic.partition.mode=nonstrict;
set mapred.job.queue.name=root.dataeng;

insert overwrite table bi_corp_common.stk_rcp_accionesemerix
partition(partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_RECUPERACIONES_CacsEmerix-Daily') }}')

SELECT
trim(nombre_accion) as ds_rcp_nombreaccion,
trim(nombre_respuesta) as ds_rcp_nombrerespuesta,
case when substring(trim(fecha_accion),1,10) in ('9999-12-31', '0001-01-01','null','') then NULL else substring(trim(fecha_accion),1,10) end as dt_rcp_fechaaccion,
case when substring(trim(fecha_respuesta),1,10) in ('9999-12-31', '0001-01-01','null','') then NULL else substring(trim(fecha_respuesta),1,10) end as dt_rcp_fecharespuesta,
cast(num_cliente as bigint) as cod_per_nup,
case when trim(contrato) in ("null","") then NULL else trim(contrato) end as ds_rcp_contratolargo,
case when trim(codigo_estado) = "Sin informar" then "S/I" else trim(codigo_estado) end as cod_rcp_estado,
trim(nombre_estado) as ds_rcp_estado,
case when trim(codigo_escenario) = "Sin informar" then "S/I" else trim(codigo_escenario) end as cod_rcp_escenario,
trim(nombre_escenario) as ds_rcp_escenario,
trim(usuario) as ds_rcp_usuario,
substr(trim(buffete), 1, 8) as cod_rcp_buffete,
substr(trim(buffete), 10) as ds_rcp_nombrebuffete,
trim(buffete) as ds_rcp_buffete,
case when trim(contrato) in ("null","") then NULL else cast(substring(contrato,9,12) as bigint) end as cod_nro_cuenta,
case when trim(contrato) in ("null","") then NULL else substring(contrato,21,2) end as cod_prod_producto,
case when trim(contrato) in ("null","") then NULL else substring(contrato,23,4) end as cod_prod_subproducto,
case when trim(contrato) in ("null","") then NULL else substring(contrato,27,3) end as cod_div_divisa,
case when trim(contrato) in ("null","") then NULL else cast(substring(contrato,5,4) as int) end as cod_suc_sucursal
from bi_corp_staging.emerix_acciones
where partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_RECUPERACIONES_CacsEmerix-Daily') }}'
;