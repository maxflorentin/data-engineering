set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=4000000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.exec.dynamic.partition.mode=nonstrict;
set mapred.job.queue.name=root.dataeng;

insert overwrite table bi_corp_common.stk_rcp_clientesemerix
partition(partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_RECUPERACIONES_CacsEmerix-Daily') }}')


SELECT
cast(substring(numero_contrato,9,12) as bigint) as cod_nro_cuenta,
substring(numero_contrato,21,2) as cod_prod_producto,
substring(numero_contrato,23,4) as cod_prod_subproducto,
substring(numero_contrato,27,3) as cod_div_divisa,
cast(substring(numero_contrato,5,4) as int) as cod_suc_sucursal,
cast(numero_cliente as bigint) as cod_per_nup,
case when trim(apellido_cliente) in ("null", "") then null else trim(apellido_cliente) end as ds_rcp_apellidocliente,
case when trim(nombre_cliente) in ("null", "") then null else trim(nombre_cliente) end as ds_rcp_nombrecliente,
case when trim(codigo_agencia) in ("null", "") then null else trim(codigo_agencia) end as cod_rcp_buffete,
case when trim(nombre_agencia) in ("null", "") then null else trim(nombre_agencia) end as ds_rcp_nombrebuffete,
case when trim(nombre_agencia) in ("null", "") or trim(codigo_agencia) in ("null", "") then null else concat(trim(nombre_agencia), ' ', trim(codigo_agencia)) end as ds_rcp_buffete,
cast(cantidad_dias_en_escenario as int) as fc_rcp_diasescenario,
cast(cantidad_dias_en_estado as int) as fc_rcp_diasestado,
case when trim(categoria_producto) in ("null", "") then null else trim(categoria_producto) end as ds_rcp_categoriaproductoemerix,
cast(deuda_contrato as decimal(17,2)) as fc_rcp_deudatotal,
cast(dias_mora_contrato as int) as fc_rcp_diasmora,
case when trim(fecha_ini_asignacion_estudio) in ("null", "") then null else regexp_replace(substring(trim(fecha_ini_asignacion_estudio),1,10),"\\/","\\-") end as dt_rcp_fechainiasignacionestudio,
case when trim(fecha_fin_asignacion_estudio) in ("null", "") then null else regexp_replace(substring(trim(fecha_fin_asignacion_estudio),1,10),"\\/","\\-") end as dt_rcp_fechafinasignacionestudio,
case when trim(codigo_escenario) in ("null", "") then null when trim(codigo_escenario) = "Sin informar" then "S/I" else trim(codigo_escenario) end as cod_rcp_escenario,
case when trim(nombre_escenario) in ("null", "") then null else trim(nombre_escenario) end as ds_rcp_escenario,
case when trim(codigo_estado) in ("null", "") then null when trim(codigo_estado) = "Sin informar" then "S/I" else trim(codigo_estado) end as cod_rcp_estado,
case when trim(nombre_estado) in ("null", "") then null else trim(nombre_estado) end as ds_rcp_estado,
cast(monto_transferido_contrato as decimal(17,2)) as fc_rcp_montotransferidocontrato,
case when trim(apellido_gestor) in ("null", "") then null else trim(apellido_gestor) end as ds_rcp_apellidogestor,
case when trim(nombre_gestor) in ("null", "") then null else trim(nombre_gestor) end as ds_rcp_nombregestor,
case when trim(motivo_salida_contrato) in ("null", "") then null else trim(motivo_salida_contrato) end as ds_rcp_motivobaja,
case when trim(numero_contrato) in ("null", "") then null else trim(numero_contrato) end as ds_rcp_contratolargo,
case when trim(pes_codigo) in ("null", "") then null else trim(pes_codigo) end as cod_rcp_segmento,
case trim(pes_obs)  when 'COMER1' then 'COMERCIO1'
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
else trim(pes_obs) end as ds_rcp_segmento,
case when trim(cod_estado_contable) in ("null", "") then null else trim(cod_estado_contable) end as cod_rcp_estadocontable,
case when trim(estado_contable) in ("null", "") then null else trim(estado_contable) end as ds_rcp_estadocontable,
case when trim(banca_nombre_corto) in ("null", "") then null else trim(banca_nombre_corto) end as cod_rcp_banca,
case when trim(codigo_garantia) in ("null", "") then null else trim(codigo_garantia) end as cod_rcp_garantia,
case when trim(nombre_garantia) in ("null", "") then null else trim(nombre_garantia) end as ds_rcp_garantias,
cast(riesgo_total_persona as decimal(17,2)) as fc_rcp_riesgototal,
case when trim(barrio_persona) in ("null", "") then null else trim(barrio_persona) end as ds_rcp_barrio,
case when trim(cliente_citinv) in ("null", "") then null else trim(cliente_citinv) end as cod_rcp_clienteciti,
case when trim(contrato_citi) in ("null", "") then null else trim(contrato_citi) end as cod_rcp_contratociti,
case when trim(fecha_ultima_consulta_nosis) in ("null", "") then null else substring(trim(fecha_ultima_consulta_nosis),1,10) end as dt_rcp_fechainfonosis,
case when trim(ingresos_inferidos) in ("null", "") then null else trim(ingresos_inferidos) end as cod_rcp_ingresosinferidos,
case when trim(endeudamiento_1) in ("null", "") then null else trim(endeudamiento_1) end as ds_rcp_endeudamiento1,
case when trim(endeudamiento_2) in ("null", "") then null else trim(endeudamiento_2) end as ds_rcp_endeudamiento2,
case when trim(endeudamiento_3) in ("null", "") then null else trim(endeudamiento_3) end as ds_rcp_endeudamiento3,
case when trim(cantidad_cheques_rechazados) in ("null", "") then null else cast(cantidad_cheques_rechazados as int) end as fc_rcp_chequesrechazados,
case when trim(monto_cheques_rechazados) in ("null", "") then null else cast(monto_cheques_rechazados as int) end as fc_rcp_montochequesrechazados,
case when trim(tiene_juicio) = 'N' then 1 else 0 end as flag_rcp_tienejuicios,
case when trim(marca_pase_judicial) = 'N' then 1 else 0 end as flag_rcp_marcapasejudicial,
case when trim(marca_venta) = 'N' then 1 else 0 end as flag_rcp_marcaventa,
case when trim(telefono_1) in ("null", "") then null else trim(telefono_1) end as ds_rcp_telefono1,
case when trim(telefono_2) in ("null", "") then null else trim(telefono_2) end as ds_rcp_telefono2,
case when trim(telefono_3) in ("null", "") then null else trim(telefono_3) end as ds_rcp_telefono3,
case when trim(valor_score) in ("null", "") then null else trim(valor_score) end as fc_rcp_score,
case when trim(valor_perju) in ("null", "") then null else trim(valor_perju) end as ds_rcp_scoreperju,
case when trim(valor_benef) in ("null", "") then null else trim(valor_benef) end as ds_rcp_scorebenef,
case when trim(script) in ("null", "") then null else trim(script) end as ds_rcp_script,
case when trim(numero_proceso) in ("null", "") then null else trim(numero_proceso) end as ds_rcp_numeroproceso,
case when trim(numero_orden_judicial) in ("null", "") then null else trim(numero_orden_judicial) end as ds_rcp_numeroordenjudicial,
case when trim(numero_posicion) in ("null", "") then null else trim(numero_posicion) end as ds_rcp_numeroposicion,
case s.segmento when 'INDIVIDUOS' then p.categoria_particulares else p.categoria_carterizados end as ds_rcp_categoriaproducto,
trim(s.segmento_control) as ds_rcp_descripcionsegmento
FROM bi_corp_staging.emerix_clientes c
LEFT JOIN bi_corp_staging.malpe_ptb_pedt030 p30 on c.numero_cliente = p30.penumper and p30.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='max_partition_malpe_ptb_pedt030', dag_id='LOAD_CMN_RECUPERACIONES_CacsEmerix-Daily') }}'
LEFT JOIN bi_corp_staging.risksql5_segmentos s ON trim(p30.pesegcla) = trim(s.ramo) and s.fecha_informacion = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_RECUPERACIONES_CacsEmerix-Daily') }}'
LEFT JOIN bi_corp_staging.risksql5_productos p ON substring(c.numero_contrato,21,2) = trim(p.codigo_producto) and substring(c.numero_contrato,23,4) = trim(p.codigo_subproducto) and p.fecha_informacion = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_RECUPERACIONES_CacsEmerix-Daily') }}'
WHERE c.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_RECUPERACIONES_CacsEmerix-Daily') }}'
;
