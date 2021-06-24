set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=4000000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.exec.dynamic.partition.mode=nonstrict;
set mapred.job.queue.name=root.dataeng;

insert overwrite table bi_corp_common.stk_rcp_rechazosderivacionemerix
partition(partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_RECUPERACIONES_CacsEmerix-Daily') }}')


SELECT
case when trim(zona_suc_adm) = "SI" then "S/I" else trim(zona_suc_adm)  end as ds_rcp_zonasucadm,
cast(trim(suc_adm) as bigint) as cod_rcp_sucadm,
trim(sucursal_nombre) as ds_rcp_sucnombre,
trim(posicion) as cod_rcp_nroposicion,
trim(tipo_doc) as cod_rcp_tipodocumento,

trim(num_doc) as ds_rcp_numerodocumento,
trim(gestor) as cod_rcp_gestor,
trim(nombre_gestor) as ds_rcp_nombregestor,
cast(num_cliente as bigint) as cod_per_nup,
trim(apellido_cliente) as ds_rcp_apellidocliente,

case when trim(nombre_cliente) in ("null","") then NULL else trim(nombre_cliente) end as ds_rcp_nombrecliente,
trim(banca) as  cod_rcp_banca,
trim(califacion) as  ds_rcp_calificacion,
case when substring(trim(fecha_ingreso_emerix),1,10) in ('9999-12-31', '0001-01-01','null','') then NULL else substring(trim(fecha_ingreso_emerix),1,10) end as dt_rcp_fechaingreso,
cast(riesgo_total as decimal(17,4)) as fc_rcp_riesgototal,

'0072' as cod_rcp_entidad,
cast(substring(contrato,5,4) as int) as cod_suc_sucursal,
cast(substring(contrato,9,12) as bigint) as cod_nro_cuenta,
substring(contrato,21,2) as cod_prod_producto,
substring(contrato,23,4) as cod_prod_subproducto,
substring(contrato,27,3) as cod_div_divisa,

trim(contrato) as ds_rcp_contratolargo,
trim(estado_contable) as ds_rcp_estadocontable,
case when trim(marca) in ("null","") then NULL else trim(marca) end as ds_rcp_marca,
case when trim(sub_marca) in ("null","") then NULL else trim(sub_marca) end as ds_rcp_submarca,
trim(garantia) as cod_rcp_garantia,

case when substring(trim(fecha_vencimiento_cuenta),1,10) in ('9999-12-31', '0001-01-01','null','') then NULL else substring(trim(fecha_vencimiento_cuenta),1,10) end as dt_rcp_fechasituacionirregular,
trim(codigo_estado) as cod_rcp_estado,
trim(nombre_estado) as ds_rcp_estado,
trim(codigo_escenario) as cod_rcp_escenario,
trim(nombre_escenario) as ds_rcp_escenario,

case when substring(trim(alta_cuenta),1,10) in ('9999-12-31', '0001-01-01','null','') then NULL else substring(trim(alta_cuenta),1,10) end as dt_rcp_fechasituacion,
case when substring(trim(fecha_vencimiento_cuenta),1,10) in ('9999-12-31', '0001-01-01','null','') then NULL else substring(trim(fecha_vencimiento_cuenta),1,10) end as dt_rcp_fechaprimerimpago,
cast(deuda_venc as decimal(17,4)) as fc_rcp_deudatotal,
cast(dias_mora_contrato as int) as fc_rcp_diasmora,
cast(cantidad_dias_en_estado as int) as fc_rcp_diasestado,

cast(cantidad_dias_en_escenario as int) as fc_rcp_diasescenario,
case trim(segmento)  when 'COMER1' then 'COMERCIO1'
                    when 'CORPOR' then 'CORPORATE'
                    when 'EMPRES' then 'EMPRESAS'
                    when 'GRAEMP' then 'GRANDES EMPRESAS'
                    when 'INDALT' then 'INDIVIDUOS RENTA ALTA'
                    when 'INDMAS' then 'INDIVIDUOS RENTA MASIVA'
                    when 'INDMED' then 'INDIVIDUOS RENTA MEDIA'
                    when 'INDSEL' then 'INDIVIDUOS RENTA SELECT'
                    when 'INSTIT' then 'INSTITUCIONES'
                    when 'null'   then NULL
                    when ''       then NULL
else trim(segmento) end as ds_rcp_segmento,
trim(paquete) as ds_rcp_paquete
from bi_corp_staging.emerix_rechazos_derivacion
where partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_RECUPERACIONES_CacsEmerix-Daily') }}'
;