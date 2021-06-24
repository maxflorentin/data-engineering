set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=4000000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.exec.dynamic.partition.mode=nonstrict;
set mapred.job.queue.name=root.dataeng;

--------------Calculo la maxima particion gsa_v_especies
CREATE TEMPORARY TABLE tmp_maxpartespecies AS
select max(partition_date) AS partition_date
  from bi_corp_Staging.gsa_v_especies
 where partition_date >= date_sub(IF('{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Inversiones') }}'<'2021-04-20','2021-04-20','{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Inversiones') }}'),7)
  and partition_date <= IF('{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Inversiones') }}'<'2021-04-20','2021-04-20','{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Inversiones') }}');


insert overwrite table bi_corp_common.dim_inv_especies_titulos
partition(partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Inversiones') }}')
select
case when trim(secid) = 'null' then NULL else trim(secid) end cod_inv_especie,
cast(oid_especie as bigint) cod_inv_oid_especie,
case when trim(tipo_especie) = 'null' then NULL else trim(tipo_especie) end ds_inv_tipo_especie,
case when trim(denominacion) = 'null' then NULL else trim(denominacion) end ds_inv_especie,
case when trim(cod_rossi) = 'null' then NULL else trim(cod_rossi) end cod_inv_rossi,
case when trim(cod_bce) = 'null' then NULL else trim(cod_bce) end cod_inv_bce,
case when trim(cod_isin)= 'null' then NULL else trim(cod_isin) end cod_inv_isin,
case when trim(cod_sam) = 'null' then NULL else trim(cod_sam) end cod_inv_sam,
case when trim(cod_bcra)  = 'null' then NULL else trim(cod_bcra) end cod_inv_bcra,
case when trim(cod_cvsa)  = 'null' then NULL else trim(cod_cvsa) end cid_inv_cvsa,
case when trim(cod_atit)  = 'null' then NULL else trim(cod_atit) end cod_inv_atit,
case when trim(cod_mae) = 'null' then NULL else trim(cod_mae) end cod_inv_mae,
case when trim(cod_fm)  = 'null' then NULL else trim(cod_fm) end cod_inv_fm,
case when trim(cod_amigo)   = 'null' then NULL else trim(cod_amigo) end cod_inv_amigo,
case when trim(pais_emisor) = 'null' then NULL else trim(pais_emisor) end cod_inv_pais_emisor,
case when trim(moneda_de_emision) = 'ARG' then 'ARS'
     when trim(moneda_de_emision) = 'Dólar' then 'USD'
     when trim(moneda_de_emision) = 'null' then null
     else trim(moneda_de_emision) end cod_inv_moneda_emision,
from_unixtime(unix_timestamp(fecha_cup_prox ,'dd-MMM-yy'),'yyyy-MM-dd') dt_inv_fecha_cup_proxima,
from_unixtime(unix_timestamp(fecha_cup_ant ,'dd-MMM-yy'),'yyyy-MM-dd') dt_inv_fecha_cup_anterior,
from_unixtime(unix_timestamp(fecha_vto ,'dd-MMM-yy'),'yyyy-MM-dd') dt_inv_fecha_vencimiento,
case when trim(periodicidad_pagos) = 'null' then NULL else trim(periodicidad_pagos) end cod_inv_periodicidad_pago,
case when trim(tipo_tasa) = 'null' then NULL else trim(tipo_tasa) end cod_inv_tipo_tasa,
cast(tasa as bigint) fc_inv_tasa,
cast(vr as bigint) fc_inv_vr,
cast(lam_min as bigint) fc_inv_lam_min,
cast(min_incre as bigint) fc_inv_min_incre,
case when trim(cash_flow) = 'null' then NULL else trim(cash_flow) end cod_inv_cash_flow,
case when trim(tipo_cap_precio) = 'null' then NULL else trim(tipo_cap_precio) end cod_inv_tipo_cap_precio,
case when trim(precio_ej) = 'null' then NULL else trim(precio_ej) end cod_inv_precio_ej,
from_unixtime(unix_timestamp(fecha_ej ,'dd-MMM-yy'),'yyyy-MM-dd') dt_inv_fecha_ej,
case when trim(per_de_liq) = 'null' then NULL else trim(per_de_liq) end cod_inv_per_de_liq,
cast(cant_por_lote as bigint) cod_inv_cant_por_lote,
 from_unixtime(unix_timestamp(dividend_date ,'dd-MMM-yy'),'yyyy-MM-dd')dt_inv_dividend_date,
case when trim(dividend) = 'null' then NULL else trim(dividend) end cod_inv_dividend,
case when trim(amb_negociacion) = 'null' then NULL else trim(amb_negociacion) end cod_inv_amb_negociacion,
case when trim(observaciones) = 'null' then NULL else trim(observaciones) end ds_inv_observaciones,
case when trim(estado) = 'null' then NULL else trim(estado) end cod_inv_estado,
case when trim(subespecie) = 'null' then NULL else trim(subespecie) end cod_inv_subespecie,
 from_unixtime(unix_timestamp(fecha_precio ,'dd-MMM-yy'),'yyyy-MM-dd')dt_inv_fecha_precio,
case when trim(plazo) = 'null' then NULL else trim(plazo) end cod_inv_plazo,
cast(titulo_publico_o_privado as bigint) cod_inv_titulo_publico_o_privado,
case when trim(lugar_default_de_custodia) = 'null' then NULL else trim(lugar_default_de_custodia) end cod_inv_lugar_default_custodia,
case when trim(sic) = 'null' then NULL else trim(sic) end cod_inv_sic,
case when trim(fte_de_precio) = 'null' then NULL else trim(fte_de_precio) end cod_inv_fte_de_precio,
case when trim(tipo_valorizacion) = 'null' then NULL else trim(tipo_valorizacion) end cod_inv_tipo_valorizacion,
case when lower(trim(moneda_de_cotizacion)) = 'null' then NULL else trim(moneda_de_cotizacion) end cod_inv_moneda_cotizacion,
case when lower(trim(clasificacion_extracto)) = 'null' then NULL else trim(clasificacion_extracto) end cod_inv_clasificacion_extracto,
case when trim(basis) = 'null' then NULL else trim(basis) end cod_inv_basis,
case when trim(unsolicited) = 'null' then NULL else trim(unsolicited) end cod_inv_unsolicited,
case when trim(prodtype) = 'null' then NULL else trim(prodtype) end cod_inv_prodtype,
case when trim(intereses_corridos) = 'null' then NULL else trim(intereses_corridos) end cod_inv_intereses_corridos,
cast(producto as bigint) cod_inv_producto,
cast(cod_categoria_cv as bigint)cod_inv_categoria_cv,
case when trim(obra_publica) = 'null' then NULL else trim(obra_publica) end cod_inv_obra_publica,
from_unixtime(unix_timestamp(fecha_hasta_amb_neg ,'dd-MMM-yy'),'yyyy-MM-dd') dt_inv_fecha_hasta_amb_neg,
case when trim(ajuste_de_capital) = 'null' then NULL else trim(ajuste_de_capital) end cod_inv_ajuste_capital,
cast(indice as bigint) cod_inv_indice
from bi_corp_staging.gsa_v_especies e
inner join tmp_maxpartespecies tmp
on (e.partition_date = tmp.partition_date);