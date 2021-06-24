set mapred.job.queue.name=root.dataeng;
set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=4000000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.auto.convert.join=true;
set hive.auto.convert.join.noconditionaltask=true;
set hive.auto.convert.join.noconditionaltask.size=40000000;

-------------------- TEMPORALES DE AUDIENCIA --------------------
DROP TABLE IF EXISTS temp_suscriptor_aux;
CREATE TEMPORARY TABLE temp_suscriptor_aux
AS
select
distinct tdgb.001_suscriptor suscriptor
from bi_corp_staging.malbgc_tbgb001 tdgb
Where cast(tdgb.001_suscriptor as int)  not in (81139, 81014, 81112, 81128,7312,7313,7305,7311,7314,81003,81005,81123,7308,13652,82004)
and tdgb.001_tpo_suscriptor in ('P','H','A','B')
and tdgb.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='max_partition_malbgc_tbgb001', dag_id='LOAD_BSN_Cuentas_Comisiones_Mantenimiento-Hist') }}';

DROP TABLE IF EXISTS temp_audiencias_aux;
CREATE TEMPORARY TABLE temp_audiencias_aux
AS
select
distinct
mco.mco_suscriptor,
mco.mco_num_convenio
from bi_corp_staging.malbgc_bgdtmco mco
where cast(mco.mco_suscriptor as int) in (81139, 81014, 81112,81128,7312,7313,7305,7311,7314,81003,81005,81123,7308,13652,82004)
and mco.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='max_partition_malbgc_bgdtmco', dag_id='LOAD_BSN_Cuentas_Comisiones_Mantenimiento-Hist') }}'
UNION ALL
select
distinct
mco2.mco_suscriptor,
mco2.mco_num_convenio
from bi_corp_staging.malbgc_bgdtmco mco2
LEFT JOIN temp_suscriptor_aux on mco2.mco_suscriptor = suscriptor
where mco2.partition_date  = '{{ ti.xcom_pull(task_ids='InputConfig', key='max_partition_malbgc_bgdtmco', dag_id='LOAD_BSN_Cuentas_Comisiones_Mantenimiento-Hist') }}';

DROP TABLE IF EXISTS  temp_audiencias;
CREATE TEMPORARY TABLE temp_audiencias
AS
SELECT 	DISTINCT
mco_suscriptor,
mco_num_convenio,
CASE WHEN cast(mco_suscriptor as int) = 81139 THEN 'Sector Público'
WHEN cast(mco_suscriptor as int) in (81014, 81112,81128,7312,7313,7305,7311,7314,81003,81005,81123,7308,13652,82004 ) then 'Senior'
WHEN cast(mco_suscriptor as int) IS NULL THEN NULL
WHEN cast(mco_suscriptor as int) not in (81139, 81014, 81112,81128,7312,7313,7305,7311,7314,81003,81005,81123,7308,13652,82004 ) then 'PAS' END as AUDIENCIA2
FROM temp_audiencias_aux;

-------------------- TEMPORALES DE PRODUCTO --------------------

DROP TABLE IF EXISTS temp_producto_arda_aux1;
CREATE TEMPORARY TABLE temp_producto_arda_aux1
AS
SELECT prod.COD_PRODUCTO, prod.DESC_PRODUCTO, prod.DESC_40, prod.DESC_20 FROM santander_business_risk_arda.producto prod
WHERE prod.DATA_DATE_PART = '{{ ti.xcom_pull(task_ids='InputConfig', key='max_partition_producto', dag_id='LOAD_BSN_Cuentas_Comisiones_Mantenimiento-Hist') }}';

DROP TABLE IF EXISTS temp_producto_subproducto_arda1;
CREATE TEMPORARY TABLE temp_producto_subproducto_arda1
AS
SELECT DISTINCT
SUBP.COD_SUBPRODU AS SUBP_COD_SUBPRO,
SUBP.DESC_20 AS SUBP_DESC,
SUBP.COD_PRODUCTO AS SUBP_COD_PRODUCTO,
PROD.DESC_PRODUCTO
FROM santander_business_risk_arda.subproducto SUBP
LEFT JOIN temp_producto_arda_aux1 PROD on SUBP.COD_PRODUCTO = PROD.COD_PRODUCTO
WHERE SUBP.DATA_DATE_PART = '{{ ti.xcom_pull(task_ids='InputConfig', key='max_partition_subproducto', dag_id='LOAD_BSN_Cuentas_Comisiones_Mantenimiento-Hist') }}';

-------------------- TEMPORALES DE ZONA --------------------

DROP TABLE IF EXISTS temp_zona_aux1;
CREATE TEMPORARY TABLE temp_zona_aux1
AS
SELECT distinct
z.SUC AS ZONA_COD_SUCURSAL,
z.ZONA AS ZONA_COD_ZONA
FROM bi_corp_staging.rio44_ba_suc_zona z
where z.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='max_partition_rio44_ba_suc_zona', dag_id='LOAD_BSN_Cuentas_Comisiones_Mantenimiento-Hist') }}';

DROP TABLE IF EXISTS temp_sucu_zona1;
CREATE TEMPORARY TABLE temp_sucu_zona1
AS
SELECT
DISTINCT
suc.cod_centro,
suc.desc_comp_centro_op AS SUCU_DESC_SUCURSAL,
aux.zona_cod_zona as zona
FROM bi_corp_staging.tcdt050 suc
LEFT JOIN temp_zona_aux1 aux on suc.cod_centro =  concat('00', aux.zona_cod_sucursal)
where suc.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='max_partition_tcdt050', dag_id='LOAD_BSN_Cuentas_Comisiones_Mantenimiento-Hist') }}';

INSERT OVERWRITE TABLE bi_corp_business.agg_comisiones_mantenimiento
PARTITION(partition_date)
SELECT
mae.CUENTA AS cod_cuenta,
mae.centro_alta AS cod_sucursal,
sz.SUCU_DESC_SUCURSAL AS ds_sucursal,
sz.zona AS cod_zona,
A.penumper AS cod_nup,
per.ds_per_segmento AS ds_segmento,
per.ds_per_subsegmento AS ds_subsegmento,
per.flag_per_mipyme AS flag_mipyme,
per.flag_per_cuentablanca AS flag_cuentablanca,
per.flag_per_vip AS flag_vip,
per.flag_per_iu AS flag_iu,
case WHEN aud.AUDIENCIA2='PAS' THEN 'PAS'
	 WHEN aud.AUDIENCIA2='Senior' THEN 'Senior'
	 WHEN aud.AUDIENCIA2='Sector Público' THEN 'Sector Público'
	 WHEN per.flag_per_iu=1 THEN 'IU'
	 WHEN per.flag_per_cuentablanca=1 THEN 'Cuenta blanca'
	 WHEN per.flag_per_vip=1 THEN 'VIP' ELSE 'Sin Audiencia' END as ds_audiencia,
IF(LCO.cod_cue_concepto IS NULL,'SIN MANT', LCO.cod_cue_concepto) AS cod_concepto,
mae.DIVISA AS cod_divisa,
mae.producto AS cod_producto,
pr.DESC_PRODUCTO AS ds_producto,
mae.subprodu AS cod_subproducto,
pr.SUBP_DESC AS ds_subproducto,
mae.plan_comi AS cod_plan,
cam.cod_cue_campania AS cod_campania,
CASE WHEN cam.cod_cue_campania IS NULL THEN 0 else 1 END AS flag_campania,
CASE
WHEN (datediff(cam.dt_cue_fecha_hasta, mae.partition_date)/30) > 18 THEN 'Mas de 18'
WHEN (datediff(cam.dt_cue_fecha_hasta, mae.partition_date)/30) >= 16
AND (datediff(cam.dt_cue_fecha_hasta, mae.partition_date)/30) <= 18 THEN '16 a 18'
WHEN (datediff(cam.dt_cue_fecha_hasta, mae.partition_date)/30) >= 13
AND (datediff(cam.dt_cue_fecha_hasta, mae.partition_date)/30) <= 15 THEN '13 a 15'
WHEN (datediff(cam.dt_cue_fecha_hasta, mae.partition_date)/30) >= 10
AND (datediff(cam.dt_cue_fecha_hasta, mae.partition_date)/30) <= 12 THEN '10 a 12'
WHEN (datediff(cam.dt_cue_fecha_hasta, mae.partition_date)/30) >= 7
AND (datediff(cam.dt_cue_fecha_hasta, mae.partition_date)/30) <= 9 THEN '7 a 9'
WHEN (datediff(cam.dt_cue_fecha_hasta, mae.partition_date)/30) >= 4
AND (datediff(cam.dt_cue_fecha_hasta, mae.partition_date)/30) <= 6 THEN '4 a 6'
WHEN (datediff(cam.dt_cue_fecha_hasta, mae.partition_date)/30) >= 1
AND (datediff(cam.dt_cue_fecha_hasta, mae.partition_date)/30) <= 3 THEN '1 a 3'
WHEN (datediff(cam.dt_cue_fecha_hasta, mae.partition_date)/30) < 1 THEN 'Ult Mes'
ELSE NULL END AS ds_mes_bonificacion,
mae.num_convenio as cod_num_convenio,
COALESCE(abs(SUM(lco.fc_cue_importe)),0) as fc_comision,
COALESCE(abs(sum(lco.fc_cue_importe_capitalizado)),0) as fc_comision_cap,
lco.dt_cue_fecha_proliq as dt_fecha_proliq,
'{{ ti.xcom_pull(task_ids='InputConfig', key='last_calendar_day', dag_id='LOAD_BSN_Cuentas_Comisiones_Mantenimiento-Hist') }}' as partition_date

FROM bi_corp_staging.bgdtmae mae
LEFT JOIN  bi_corp_common.bt_cue_comisiones_cobradas lco on LCO.cod_suc_sucursal_cargo = mae.centro_alta
										AND LCO.cod_cue_cuenta_cargo = mae.CUENTA
										AND LCO.cod_cue_div_cargo = mae.DIVISA
										AND lco.cod_cue_concepto= 'MANT'
										AND SUBSTRING(lco.partition_date, 1, 7) = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='LOAD_BSN_Cuentas_Comisiones_Mantenimiento-Hist') }}'

LEFT JOIN bi_corp_staging.malpe_ptb_pedt008 A on mae.cuenta = concat('0', A.pecodpro, SUBSTRING(A.penumcon, 4))
										AND mae.centro_alta = A.pecodofi
										AND A.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='max_partition_malpe_ptb_pedt008', dag_id='LOAD_BSN_Cuentas_Comisiones_Mantenimiento-Hist') }}'
										AND A.pecdgent = '0072'
										AND A.pecalpar = 'TI'
										AND A.pecodpro in ('02', '03','05', '06', '07')

LEFT JOIN bi_corp_common.stk_per_personas PER on PER.COD_PER_NUP= cast(A.penumper as int)
										AND per.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='max_partition_stk_per_personas', dag_id='LOAD_BSN_Cuentas_Comisiones_Mantenimiento-Hist') }}'

LEFT JOIN bi_corp_common.stk_cue_campanias cam ON mae.CUENTA = cam.cod_cue_cuenta
												AND mae.centro_alta = cam.cod_suc_sucursal
												AND mae.DIVISA= cam.cod_cue_divisa
												AND cam.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='max_partition_stk_cue_campanias', dag_id='LOAD_BSN_Cuentas_Comisiones_Mantenimiento-Hist') }}'

LEFT JOIN temp_sucu_zona1 sz ON CAST(mae.centro_alta AS INT) = CAST(sz.cod_centro AS INT)
LEFT JOIN temp_producto_subproducto_arda1 pr ON pr.SUBP_COD_SUBPRO = mae.subprodu_contab
											AND pr.SUBP_COD_PRODUCTO = mae.producto_contab

LEFT JOIN temp_audiencias aud ON aud.mco_num_convenio = mae.num_convenio

WHERE mae.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='max_partition_bgdtmae', dag_id='LOAD_BSN_Cuentas_Comisiones_Mantenimiento-Hist') }}'
AND mae.indesta = 'A'
AND (mae.producto IN ('03','05', '06', '07') or (mae.producto = '02' and mae.subprodu <> '0018'))
and mae.divisa = 'ARS'
GROUP BY
mae.CUENTA,
mae.centro_alta,
sz.SUCU_DESC_SUCURSAL,
sz.zona,
A.penumper,
per.ds_per_segmento,
per.ds_per_subsegmento,
per.flag_per_mipyme,
per.flag_per_cuentablanca,
per.flag_per_vip,
per.flag_per_iu,
case WHEN aud.AUDIENCIA2='PAS' THEN 'PAS'
	 WHEN aud.AUDIENCIA2='Senior' THEN 'Senior'
	 WHEN aud.AUDIENCIA2='Sector Público' THEN 'Sector Público'
	 WHEN per.flag_per_iu=1 THEN 'IU'
	 WHEN per.flag_per_cuentablanca=1 THEN 'Cuenta blanca'
	 WHEN per.flag_per_vip=1 THEN 'VIP' ELSE 'Sin Audiencia' END,
IF(LCO.cod_cue_concepto IS NULL,'SIN MANT', LCO.cod_cue_concepto),
mae.DIVISA,
mae.producto,
pr.DESC_PRODUCTO,
mae.subprodu,
pr.SUBP_DESC,
mae.plan_comi,
cam.cod_cue_campania,
CASE WHEN cam.cod_cue_campania IS NULL THEN 0 else 1 END,
CASE
WHEN (datediff(cam.dt_cue_fecha_hasta, mae.partition_date)/30) > 18 THEN 'Mas de 18'
WHEN (datediff(cam.dt_cue_fecha_hasta, mae.partition_date)/30) >= 16
AND (datediff(cam.dt_cue_fecha_hasta, mae.partition_date)/30) <= 18 THEN '16 a 18'
WHEN (datediff(cam.dt_cue_fecha_hasta, mae.partition_date)/30) >= 13
AND (datediff(cam.dt_cue_fecha_hasta, mae.partition_date)/30) <= 15 THEN '13 a 15'
WHEN (datediff(cam.dt_cue_fecha_hasta, mae.partition_date)/30) >= 10
AND (datediff(cam.dt_cue_fecha_hasta, mae.partition_date)/30) <= 12 THEN '10 a 12'
WHEN (datediff(cam.dt_cue_fecha_hasta, mae.partition_date)/30) >= 7
AND (datediff(cam.dt_cue_fecha_hasta, mae.partition_date)/30) <= 9 THEN '7 a 9'
WHEN (datediff(cam.dt_cue_fecha_hasta, mae.partition_date)/30) >= 4
AND (datediff(cam.dt_cue_fecha_hasta, mae.partition_date)/30) <= 6 THEN '4 a 6'
WHEN (datediff(cam.dt_cue_fecha_hasta, mae.partition_date)/30) >= 1
AND (datediff(cam.dt_cue_fecha_hasta, mae.partition_date)/30) <= 3 THEN '1 a 3'
WHEN (datediff(cam.dt_cue_fecha_hasta, mae.partition_date)/30) < 1 THEN 'Ult Mes'
ELSE NULL END,
mae.num_convenio,
lco.dt_cue_fecha_proliq,
'{{ ti.xcom_pull(task_ids='InputConfig', key='last_calendar_day', dag_id='LOAD_BSN_Cuentas_Comisiones_Mantenimiento-Hist') }}'
