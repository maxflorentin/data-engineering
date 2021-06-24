set mapred.job.queue.name=root.dataeng;
set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=4000000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.auto.convert.join=true;
set hive.auto.convert.join.noconditionaltask=true;
set hive.auto.convert.join.noconditionaltask.size=40000000;

DROP TABLE IF EXISTS temp_volumenes_aux;
CREATE TEMPORARY TABLE temp_volumenes_aux
AS
SELECT
year(c.dt_cue_valor)*100+month(c.dt_cue_valor) as PERIODO,
c.cod_cue_cuenta CUENTA,
c.cod_suc_sucursal CENTRO,
c.cod_suc_sucursal_ultimo_movimiento,
c.dt_cue_valor FECHA,
c.cod_cue_producto PRODUCTO,
c.cod_cue_subproducto SUBPRODUCTO,
c.cod_cue_divisa MONEDA,
CASE WHEN c.cod_cue_codigo in ('0301', '5301', '0302', '5302', '0303', '5303','0304', '5304', '0305', '5305', '0306', '5306', '0308', '5308', '0471', '5471', '0686', '5686','1533','6533', '2383', '7383','2385', '7385', '2494', '7494', '4520', '9520', '4562', '9562', '4756', '9756') THEN 'EXTRACCIONES' ELSE 'DEPOSITOS' END AS CONCEPTO,
abs(sum(c.fc_cue_importe)) AS IMPORTE
FROM  bi_corp_common.bt_cue_movimientos_cuenta c
WHERE c.cod_cue_codigo in ('0301', '5301', '0302', '5302', '0303', '5303','0304', '5304', '0305', '5305', '0306', '5306', '0308', '5308', '0471', '5471', '0686', '5686','1533','6533', '2383', '7383','2385', '7385', '2494', '7494', '4520', '9520', '4562', '9562', '4756', '9756', '0032', '5032', '0038', '5038', '0039', '5039', '0047', '5047', '0469', '5469', '0799', '5799', '0800', '5800', '1293', '6293', '1294', '6294', '1295', '6295', '1296', '6296', '1532', '6532', '1700', '6700', '1937', '6937', '1938', '6938', '2029', '7029', '2030', '7030', '2077', '7077', '2079', '7079', '4398', '9398', '4522', '9522', '4746', '9746')
and c.cod_cue_producto in ('02', '03','05', '06', '07')
and c.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_BSN_Cuentas_Comisiones_Billetes-Daily') }}'
GROUP BY year(c.dt_cue_valor)*100+month(c.dt_cue_valor), c.cod_cue_cuenta, c.cod_suc_sucursal, c.cod_suc_sucursal_ultimo_movimiento, c.dt_cue_valor, c.cod_cue_producto, c.cod_cue_subproducto, c.cod_cue_divisa,
CASE WHEN c.cod_cue_codigo in ('0301', '5301', '0302', '5302', '0303', '5303','0304', '5304', '0305', '5305', '0306', '5306', '0308', '5308', '0471', '5471', '0686', '5686','1533','6533', '2383', '7383','2385', '7385', '2494', '7494', '4520', '9520', '4562', '9562', '4756', '9756') THEN 'EXTRACCIONES' ELSE 'DEPOSITOS' end;

DROP TABLE IF EXISTS temp_volumenes;
CREATE TEMPORARY TABLE temp_volumenes
AS
SELECT
PERIODO,
CUENTA,
CENTRO,
--cod_suc_sucursal_ultimo_movimiento CENTRO_MOV,
FECHA,
PRODUCTO,
SUBPRODUCTO,
MONEDA,
CONCEPTO,
CASE WHEN concepto='DEPOSITOS' and ((CENTRO = cod_suc_sucursal_ultimo_movimiento)
OR (cod_suc_sucursal_ultimo_movimiento=773 and CENTRO in (69, 62, 365, 230,225)) OR (cod_suc_sucursal_ultimo_movimiento=781 and CENTRO= 360) OR (cod_suc_sucursal_ultimo_movimiento=744 and CENTRO= 533) OR (cod_suc_sucursal_ultimo_movimiento=771 and CENTRO= 518) OR (cod_suc_sucursal_ultimo_movimiento=780 and CENTRO= 371)
OR (cod_suc_sucursal_ultimo_movimiento=770 and CENTRO= 179) OR (cod_suc_sucursal_ultimo_movimiento=777 and CENTRO= 67) OR (cod_suc_sucursal_ultimo_movimiento=775 and CENTRO= 60) OR (cod_suc_sucursal_ultimo_movimiento=772 and CENTRO= 53) OR (cod_suc_sucursal_ultimo_movimiento=779 and CENTRO= 13) OR (cod_suc_sucursal_ultimo_movimiento=0 and CENTRO in (2245, 2272))) THEN 'DEPE'
WHEN concepto='DEPOSITOS' AND CENTRO <> cod_suc_sucursal_ultimo_movimiento THEN 'RIOM' ELSE 'EXT' END AS TIPO_OPERACION,
sum(IMPORTE) as IMPORTE
FROM temp_volumenes_aux
GROUP BY PERIODO,CUENTA,CENTRO,
--cod_suc_sucursal_ultimo_movimiento,
FECHA,PRODUCTO,SUBPRODUCTO,MONEDA,CONCEPTO,CASE WHEN concepto='DEPOSITOS' and ((CENTRO = cod_suc_sucursal_ultimo_movimiento)
OR (cod_suc_sucursal_ultimo_movimiento=773 and CENTRO in (69, 62, 365, 230,225)) OR (cod_suc_sucursal_ultimo_movimiento=781 and CENTRO= 360) OR (cod_suc_sucursal_ultimo_movimiento=744 and CENTRO= 533) OR (cod_suc_sucursal_ultimo_movimiento=771 and CENTRO= 518) OR (cod_suc_sucursal_ultimo_movimiento=780 and CENTRO= 371)
OR (cod_suc_sucursal_ultimo_movimiento=770 and CENTRO= 179) OR (cod_suc_sucursal_ultimo_movimiento=777 and CENTRO= 67) OR (cod_suc_sucursal_ultimo_movimiento=775 and CENTRO= 60) OR (cod_suc_sucursal_ultimo_movimiento=772 and CENTRO= 53) OR (cod_suc_sucursal_ultimo_movimiento=779 and CENTRO= 13) OR (cod_suc_sucursal_ultimo_movimiento=0 and CENTRO in (2245, 2272))) THEN 'DEPE'
WHEN concepto='DEPOSITOS' AND CENTRO <> cod_suc_sucursal_ultimo_movimiento THEN 'RIOM' ELSE 'EXT' END;

DROP TABLE IF EXISTS temp_producto_arda_aux;
CREATE TEMPORARY TABLE temp_producto_arda_aux
AS
SELECT prod.COD_PRODUCTO, prod.DESC_PRODUCTO, prod.DESC_40, prod.DESC_20 FROM santander_business_risk_arda.producto prod
WHERE prod.DATA_DATE_PART = '{{ ti.xcom_pull(task_ids='InputConfig', key='max_partition_producto', dag_id='LOAD_BSN_Cuentas_Comisiones_Billetes-Daily') }}';

DROP TABLE IF EXISTS temp_producto_subproducto_arda;
CREATE TEMPORARY TABLE temp_producto_subproducto_arda
AS
SELECT DISTINCT
SUBP.COD_SUBPRODU AS SUBP_COD_SUBPRO,
SUBP.DESC_20 AS SUBP_DESC,
SUBP.COD_PRODUCTO AS SUBP_COD_PRODUCTO,
PROD.DESC_PRODUCTO
FROM santander_business_risk_arda.subproducto SUBP
LEFT JOIN temp_producto_arda_aux PROD on SUBP.COD_PRODUCTO = PROD.COD_PRODUCTO
WHERE SUBP.DATA_DATE_PART = '{{ ti.xcom_pull(task_ids='InputConfig', key='max_partition_subproducto', dag_id='LOAD_BSN_Cuentas_Comisiones_Billetes-Daily') }}';

DROP TABLE IF EXISTS temp_zona_aux;
CREATE TEMPORARY TABLE temp_zona_aux
AS
SELECT distinct
z.SUC AS ZONA_COD_SUCURSAL,
z.ZONA AS ZONA_COD_ZONA
FROM bi_corp_staging.rio44_ba_suc_zona z
where z.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='max_partition_rio44_ba_suc_zona', dag_id='LOAD_BSN_Cuentas_Comisiones_Billetes-Daily') }}';

DROP TABLE IF EXISTS temp_sucu_zona;
CREATE TEMPORARY TABLE temp_sucu_zona
AS
SELECT
DISTINCT
suc.cod_centro,
suc.desc_comp_centro_op AS SUCU_DESC_SUCURSAL,
aux.zona_cod_zona as zona
FROM bi_corp_staging.tcdt050 suc
LEFT JOIN temp_zona_aux aux on suc.cod_centro =  concat('00', aux.zona_cod_sucursal)
where suc.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='max_partition_tcdt050', dag_id='LOAD_BSN_Cuentas_Comisiones_Billetes-Daily') }}';

INSERT OVERWRITE TABLE bi_corp_business.agg_comisiones_billetes
PARTITION(partition_date)
SELECT
IMP.CUENTA AS cod_cuenta,
IMP.CENTRO AS cod_sucursal,
sz.SUCU_DESC_SUCURSAL AS ds_sucursal,
sz.zona AS cod_zona,
--CASE
--WHEN TIPO_OPERACION = 'RIOM' AND sz.zona <> sz2.zona THEN 'EXTRA'
--WHEN TIPO_OPERACION = 'RIOM' AND sz.zona = sz2.zona THEN 'INTRA'
--ELSE NULL END AS ds_tipo_zona,
NULL AS ds_tipo_zona,
A.penumper AS cod_nup,
per.ds_per_segmento AS ds_segmento,
per.ds_per_subsegmento AS ds_subsegmento,
per.flag_per_mipyme AS flag_mipyme,
IMP.CONCEPTO as cod_concepto,
--IMP.TIPO_OPERACION AS ds_operacion,
NULL AS ds_operacion,
IMP.MONEDA AS cod_divisa,
IMP.PRODUCTO AS cod_producto,
pr.DESC_PRODUCTO AS ds_producto,
IMP.SUBPRODUCTO AS cod_subproducto,
pr.SUBP_DESC AS ds_subproducto,
mae.plan_comi AS cod_plan,
cam.cod_cue_campania AS cod_campania,
CASE WHEN cam.cod_cue_campania IS NULL THEN 0 else 1 END AS flag_campania,
CASE
WHEN (datediff(cam.dt_cue_fecha_hasta,IMP.FECHA)/30) > 18 THEN 'Mas de 18'
WHEN (datediff(cam.dt_cue_fecha_hasta,IMP.FECHA)/30) >= 16
AND (datediff(cam.dt_cue_fecha_hasta,IMP.FECHA)/30) <= 18 THEN '16 a 18'
WHEN (datediff(cam.dt_cue_fecha_hasta,IMP.FECHA)/30) >= 13
AND (datediff(cam.dt_cue_fecha_hasta,IMP.FECHA)/30) <= 15 THEN '13 a 15'
WHEN (datediff(cam.dt_cue_fecha_hasta,IMP.FECHA)/30) >= 10
AND (datediff(cam.dt_cue_fecha_hasta,IMP.FECHA)/30) <= 12 THEN '10 a 12'
WHEN (datediff(cam.dt_cue_fecha_hasta,IMP.FECHA)/30) >= 7
AND (datediff(cam.dt_cue_fecha_hasta,IMP.FECHA)/30) <= 9 THEN '7 a 9'
WHEN (datediff(cam.dt_cue_fecha_hasta,IMP.FECHA)/30) >= 4
AND (datediff(cam.dt_cue_fecha_hasta,IMP.FECHA)/30) <= 6 THEN '4 a 6'
WHEN (datediff(cam.dt_cue_fecha_hasta,IMP.FECHA)/30) >= 1
AND (datediff(cam.dt_cue_fecha_hasta,IMP.FECHA)/30) <= 3 THEN '1 a 3'
WHEN (datediff(cam.dt_cue_fecha_hasta,IMP.FECHA)/30) < 1 THEN 'Ult Mes'
ELSE NULL END AS ds_mes_bonificacion,
COALESCE(abs(SUM(IMP.IMPORTE)),0) AS fc_volumen,
COALESCE(abs(SUM(lco.fc_cue_importe)),0) as fc_comision,
COALESCE(abs(sum(lco.fc_cue_importe_capitalizado)),0) as fc_comision_cap,
IMP.FECHA as dt_fecha,
'{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_BSN_Cuentas_Comisiones_Billetes-Daily') }}' as partition_date

FROM
(
select
FECHA,
PERIODO,
MONEDA,
CUENTA,
CENTRO,
--CENTRO_MOV,
PRODUCTO,
SUBPRODUCTO,
CONCEPTO,
--TIPO_OPERACION,
abs(sum(importe)) as IMPORTE
from temp_volumenes
group by FECHA, PERIODO, MONEDA, CUENTA, CENTRO,
--CENTRO_MOV,
PRODUCTO, SUBPRODUCTO, CONCEPTO--, TIPO_OPERACION
) IMP
LEFT JOIN ( SELECT cod_cue_cuenta, cod_suc_sucursal, cod_cue_divisa, dt_cue_fecha_mov, cod_cue_producto,cod_cue_subproducto,
					CASE WHEN cod_cue_concepto in ('DEPE', 'RIOM') THEN 'DEPOSITOS'
					WHEN cod_cue_concepto in ('EXT') THEN 'EXTRACCIONES' END AS concepto,
					sum(fc_cue_importe) AS fc_cue_importe,
					sum(fc_cue_importe_capitalizado) AS fc_cue_importe_capitalizado
					FROM bi_corp_common.bt_cue_comisiones_cobradas
					WHERE partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_BSN_Cuentas_Comisiones_Billetes-Daily') }}'
					AND cod_cue_concepto in ('DEPE', 'RIOM', 'EXT')
					GROUP BY  cod_cue_cuenta, cod_suc_sucursal, cod_cue_divisa, dt_cue_fecha_mov, cod_cue_producto,cod_cue_subproducto,
					CASE WHEN cod_cue_concepto in ('DEPE', 'RIOM') THEN 'DEPOSITOS'
					WHEN cod_cue_concepto in ('EXT') THEN 'EXTRACCIONES' END)  LCO
					ON LCO.cod_cue_cuenta = IMP.CUENTA
					AND LCO.cod_suc_sucursal = cast(IMP.CENTRO as int)
					AND LCO.cod_cue_divisa = IMP.MONEDA
					AND lco.dt_cue_fecha_mov = IMP.FECHA
					AND lco.cod_cue_producto = IMP.PRODUCTO
					AND lco.cod_cue_subproducto = IMP.SUBPRODUCTO
					AND lco.concepto = IMP.CONCEPTO
LEFT JOIN bi_corp_staging.bgdtmae mae on IMP.CENTRO = mae.centro_alta
										AND IMP.CUENTA = mae.CUENTA
										AND IMP.MONEDA = mae.DIVISA
										AND mae.indesta = 'A'
										AND mae.producto IN ('02', '03','05', '06', '07')
										AND mae.producto = IMP.producto
										AND mae.subprodu = imp.subproducto
										AND mae.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='max_partition_bgdtmae', dag_id='LOAD_BSN_Cuentas_Comisiones_Billetes-Daily') }}'
LEFT JOIN bi_corp_staging.malpe_ptb_pedt008 A on imp.CUENTA = concat('0', A.pecodpro, SUBSTRING(A.penumcon, 4))
											AND imp.CENTRO = A.pecodofi
											AND A.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='max_partition_malpe_ptb_pedt008', dag_id='LOAD_BSN_Cuentas_Comisiones_Billetes-Daily') }}'
											AND A.pecdgent = '0072'
											AND A.pecalpar = 'TI'
											AND A.pecodpro in ('02', '03','05', '06', '07')
LEFT JOIN bi_corp_common.stk_per_personas PER on PER.COD_PER_NUP= cast(A.penumper as int)
											AND per.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='max_partition_stk_per_personas', dag_id='LOAD_BSN_Cuentas_Comisiones_Billetes-Daily') }}'
LEFT JOIN bi_corp_common.stk_cue_campanias cam ON IMP.CUENTA = cam.cod_cue_cuenta
											AND IMP.CENTRO = cam.cod_suc_sucursal
											AND IMP.MONEDA = cam.cod_cue_divisa
											AND IMP.FECHA = cam.partition_date
											AND IMP.producto = cam.cod_cue_producto
											AND imp.subproducto = cam.cod_cue_subproducto
											AND cam.partition_date= '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_BSN_Cuentas_Comisiones_Billetes-Daily') }}'
LEFT JOIN temp_sucu_zona sz ON IMP.CENTRO = sz.cod_centro
-- LEFT JOIN temp_sucu_zona sz2 ON IMP.CENTRO_MOV = sz2.cod_centro
LEFT JOIN temp_producto_subproducto_arda pr ON pr.SUBP_COD_PRODUCTO = IMP.PRODUCTO
											AND pr.SUBP_COD_SUBPRO = IMP.SUBPRODUCTO
GROUP BY
IMP.CUENTA,
IMP.CENTRO,
sz.SUCU_DESC_SUCURSAL,
sz.zona,
--CASE
--WHEN TIPO_OPERACION = 'RIOM' AND sz.zona <> sz2.zona THEN 'EXTRA'
--WHEN TIPO_OPERACION = 'RIOM' AND sz.zona = sz2.zona THEN 'INTRA'
--ELSE NULL END,
NULL,
A.penumper,
per.ds_per_segmento,
per.ds_per_subsegmento,
per.flag_per_mipyme,
IMP.CONCEPTO,
--IMP.TIPO_OPERACION,
NULL,
IMP.MONEDA,
IMP.PRODUCTO,
pr.DESC_PRODUCTO,
IMP.SUBPRODUCTO,
pr.SUBP_DESC,
mae.plan_comi,
cam.cod_cue_campania,
CASE WHEN cam.cod_cue_campania IS NULL THEN 0 else 1 END,
CASE
WHEN (datediff(cam.dt_cue_fecha_hasta,IMP.FECHA)/30) > 18 THEN 'Mas de 18'
WHEN (datediff(cam.dt_cue_fecha_hasta,IMP.FECHA)/30) >= 16
AND (datediff(cam.dt_cue_fecha_hasta,IMP.FECHA)/30) <= 18 THEN '16 a 18'
WHEN (datediff(cam.dt_cue_fecha_hasta,IMP.FECHA)/30) >= 13
AND (datediff(cam.dt_cue_fecha_hasta,IMP.FECHA)/30) <= 15 THEN '13 a 15'
WHEN (datediff(cam.dt_cue_fecha_hasta,IMP.FECHA)/30) >= 10
AND (datediff(cam.dt_cue_fecha_hasta,IMP.FECHA)/30) <= 12 THEN '10 a 12'
WHEN (datediff(cam.dt_cue_fecha_hasta,IMP.FECHA)/30) >= 7
AND (datediff(cam.dt_cue_fecha_hasta,IMP.FECHA)/30) <= 9 THEN '7 a 9'
WHEN (datediff(cam.dt_cue_fecha_hasta,IMP.FECHA)/30) >= 4
AND (datediff(cam.dt_cue_fecha_hasta,IMP.FECHA)/30) <= 6 THEN '4 a 6'
WHEN (datediff(cam.dt_cue_fecha_hasta,IMP.FECHA)/30) >= 1
AND (datediff(cam.dt_cue_fecha_hasta,IMP.FECHA)/30) <= 3 THEN '1 a 3'
WHEN (datediff(cam.dt_cue_fecha_hasta,IMP.FECHA)/30) < 1 THEN 'Ult Mes'
ELSE NULL END,
IMP.FECHA,
'{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_BSN_Cuentas_Comisiones_Billetes-Daily') }}'