set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=4000000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.exec.dynamic.partition.mode=nonstrict;
set mapred.job.queue.name=root.dataeng;

-- REFINANCIACIONES

CREATE TEMPORARY TABLE contratos_ref AS
select
case when trim(r.`086_num_persona`) != '' then trim(r.`086_num_persona`) else trim(p8.penumper) end as nup,
r.`086_cod_entidad` dor_entidad,
r.`086_cod_centro` dor_sucursal,
r.`086_num_contrato` dor_cuenta,
r.`086_cod_producto` dor_producto,
r.`086_cod_subprodu` dor_subprodu,
r.`086_cod_divisa` dor_divisa,
r.`086_cod_centrod` ado_sucursal,
r.`086_num_contratd` ado_cuenta,
r.`086_cod_productd` ado_producto,
r.`086_cod_subprodd` ado_subprodu,
r.`086_cod_divisad` ado_divisa,
r.`086_cod_reesctr` as tipo_reestructuracion,
trim(p.categoria_particulares) as agrupacion_producto,
r.`086_imp_refnacdo` imp_refinanciado,
sum(r.`086_imp_refnacdo`) over(partition by r.`086_num_persona`, r.`086_cod_entidad`, r.`086_cod_centro`, r.`086_num_contrato`, r.`086_cod_producto`, r.`086_cod_subprodu`, r.`086_cod_divisa`, p.categoria_particulares) as imp_agrupacion_producto,
sum(r.`086_imp_refnacdo`) over(partition by r.`086_num_persona`, r.`086_cod_entidad`, r.`086_cod_centro`, r.`086_num_contrato`, r.`086_cod_producto`, r.`086_cod_subprodu`, r.`086_cod_divisa`) as imp_total,
r.`086_fec_refinanc` as fec_refinanciacion
from bi_corp_staging.garra_contratos_ref r
left join bi_corp_staging.risksql5_productos p on trim(p.codigo_producto) = trim(r.`086_cod_productd`) and trim(p.codigo_subproducto) = trim(r.`086_cod_subprodd`) and p.fecha_informacion = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_MORA_PrimasDistribuidas-Daily') }}'
left join bi_corp_staging.malpe_pedt008 p8 on p8.penumcon = r.`086_num_contrato` and r.`086_cod_centro` = p8.pecodofi and r.`086_cod_producto` = p8.pecodpro and p8.pecalpar = 'TI' and p8.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_MORA_PrimasDistribuidas-Daily') }}'
where r.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_MORA_PrimasDistribuidas-Daily') }}'
;

CREATE TEMPORARY TABLE ref_pivote_prev AS
SELECT
c.nup,
c.dor_sucursal,
c.dor_cuenta,
c.dor_producto,
c.dor_subprodu,
c.dor_divisa,
p.descripcion,
p.categoria_particulares,
p.categoria_carterizados,
p.refinanciacion,
c.tipo_reestructuracion,
c.imp_refinanciado,
c.imp_agrupacion_producto,
c.imp_total,
SUM(CASE WHEN agrupacion_producto in ('CUENTA CORRIENTE', 'PLAZO FIJO') THEN ( imp_refinanciado / imp_total ) * 100 END) over(partition by nup, dor_sucursal, dor_cuenta, dor_producto, dor_subprodu, dor_divisa) AS porc_cuenta_corriente,
SUM(CASE agrupacion_producto WHEN 'TARJETAS' THEN ( imp_refinanciado / imp_total ) * 100 END) over(partition by nup, dor_sucursal, dor_cuenta, dor_producto, dor_subprodu, dor_divisa) AS porc_tarjetas,
SUM(CASE agrupacion_producto WHEN 'PERSONALES' THEN ( imp_refinanciado / imp_total ) * 100 END) over(partition by nup, dor_sucursal, dor_cuenta, dor_producto, dor_subprodu, dor_divisa) AS porc_personales,
SUM(CASE agrupacion_producto WHEN 'OTROS' THEN ( imp_refinanciado / imp_total ) * 100 END) over(partition by nup, dor_sucursal, dor_cuenta, dor_producto, dor_subprodu, dor_divisa) AS porc_otros,
SUM(CASE agrupacion_producto WHEN 'HIPOTECARIO' THEN ( imp_refinanciado / imp_total ) * 100 END) over(partition by nup, dor_sucursal, dor_cuenta, dor_producto, dor_subprodu, dor_divisa) AS porc_prestamo_hipotecario,
SUM(CASE agrupacion_producto WHEN 'PRENDARIO' THEN ( imp_refinanciado / imp_total ) * 100 END) over(partition by nup, dor_sucursal, dor_cuenta, dor_producto, dor_subprodu, dor_divisa) AS porc_prestamo_prendario,--SUM(CASE WHEN agrupacion_producto in ('CUENTA CORRIENTE', 'PLAZO FIJO') THEN ( imp_refinanciado / imp_total ) * 100 END) over(partition by nup, dor_sucursal, dor_cuenta, dor_producto, dor_subprodu, dor_divisa) AS porc_cuenta_corriente,
coalesce(SUM(CASE WHEN agrupacion_producto in ('CUENTA CORRIENTE', 'PLAZO FIJO') THEN ( imp_refinanciado / imp_total ) * 100 END) over(partition by nup, dor_sucursal, dor_cuenta, dor_producto, dor_subprodu, dor_divisa), 0) +
coalesce(SUM(CASE agrupacion_producto WHEN 'TARJETAS' THEN ( imp_refinanciado / imp_total ) * 100 END) over(partition by nup, dor_sucursal, dor_cuenta, dor_producto, dor_subprodu, dor_divisa), 0) +
coalesce(SUM(CASE agrupacion_producto WHEN 'PERSONALES' THEN ( imp_refinanciado / imp_total ) * 100 END) over(partition by nup, dor_sucursal, dor_cuenta, dor_producto, dor_subprodu, dor_divisa), 0) +
coalesce(SUM(CASE agrupacion_producto WHEN 'OTROS' THEN ( imp_refinanciado / imp_total ) * 100 END) over(partition by nup, dor_sucursal, dor_cuenta, dor_producto, dor_subprodu, dor_divisa), 0) +
coalesce(SUM(CASE agrupacion_producto WHEN 'HIPOTECARIO' THEN ( imp_refinanciado / imp_total ) * 100 END) over(partition by nup, dor_sucursal, dor_cuenta, dor_producto, dor_subprodu, dor_divisa), 0) +
coalesce(SUM(CASE agrupacion_producto WHEN 'PRENDARIO' THEN ( imp_refinanciado / imp_total ) * 100 END) over(partition by nup, dor_sucursal, dor_cuenta, dor_producto, dor_subprodu, dor_divisa), 0) AS suma,
c.fec_refinanciacion,
0 as distribuido_por_cierre_controlado,
1 as distribuido_por_contratos_ref
FROM contratos_ref c
left join bi_corp_staging.risksql5_productos p on trim(p.codigo_producto) = trim(c.dor_producto) and trim(p.codigo_subproducto) = trim(c.dor_subprodu) and p.fecha_informacion = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_MORA_PrimasDistribuidas-Daily') }}'
;

CREATE TEMPORARY TABLE ref_pivote AS
SELECT
nup,
dor_sucursal,
dor_cuenta,
dor_producto,
dor_subprodu,
dor_divisa,
descripcion,
categoria_particulares,
categoria_carterizados,
refinanciacion,
tipo_reestructuracion,
imp_refinanciado,
imp_agrupacion_producto,
imp_total,
case when c.suma = 0 then cast(g.porc_cuenta_cte as decimal(38,2)) else cast(c.porc_cuenta_corriente as decimal(38,2)) end as porc_cuenta_corriente,
case when c.suma = 0 then cast(g.porc_tarjetas as decimal(38,2)) else cast(c.porc_tarjetas as decimal(38,2)) end as porc_tarjetas,
case when c.suma = 0 then cast(g.porc_personales as decimal(38,2)) else cast(c.porc_personales as decimal(38,2)) end as porc_personales,
case when c.suma = 0 then cast(g.porc_otros as decimal(38,2)) else cast(c.porc_otros as decimal(38,2)) end as porc_otros,
case when c.suma = 0 then cast(g.porc_hipotecario as decimal(38,2)) else cast(c.porc_prestamo_hipotecario as decimal(38,2)) end as porc_prestamo_hipotecario,
case when c.suma = 0 then cast(g.porc_prendario as decimal(38,2)) else cast(c.porc_prestamo_prendario as decimal(38,2)) end as porc_prestamo_prendario,0 as distribuido_por_cierre_controlado,
1 as distribuido_por_contratos_ref
FROM ref_pivote_prev c
left join bi_corp_common.dim_mora_primas_genericas g on g.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_MORA_PrimasDistribuidas-Daily') }}' and g.periodo = regexp_replace(substr(c.fec_refinanciacion , 1, 7), "-", "") and g.distribucion = 'c'
;

CREATE TEMPORARY TABLE ref_mora_pivote AS
SELECT
nup,
dor_sucursal,
dor_cuenta,
p.codigo_producto_mora as dor_producto,
p.codigo_subproducto_mora as dor_subprodu,
dor_divisa,
c.descripcion,
c.categoria_particulares,
c.categoria_carterizados,
c.refinanciacion,
c.tipo_reestructuracion,
imp_refinanciado,
imp_agrupacion_producto,
imp_total,
porc_cuenta_corriente,
porc_tarjetas,
porc_personales,
porc_otros,
porc_prestamo_hipotecario,
porc_prestamo_prendario,
distribuido_por_cierre_controlado,
distribuido_por_contratos_ref
FROM ref_pivote c
left join bi_corp_staging.risksql5_productos p on trim(p.codigo_producto) = trim(c.dor_producto) and trim(p.codigo_subproducto) = trim(c.dor_subprodu) and p.fecha_informacion = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_MORA_PrimasDistribuidas-Daily') }}'
;



-- CIERRE CONTROLADOS

CREATE TEMPORARY TABLE contratos_cru AS
select c.num_persona as nup,
c.nro_propuesta,
c.cod_entidad,
c.cod_centro as dor_sucursal,
c.num_contrato as dor_cuenta,
c.cod_producto as dor_producto,
c.cod_subprodu as dor_subprodu,
c.cod_divisa as dor_divisa,
c.cod_centro_rel,
c.num_contrato_rel,
c.cod_producto_rel,
c.cod_subprodu_rel,
c.cod_divisa_rel,
trim(p.categoria_particulares) as agrupacion_producto,
cast(REGEXP_REPLACE(c.porc_deuda, ",",".") as decimal(17,2)) AS porc_deuda,
cast(REGEXP_REPLACE(c.imp_saldo_cierre, ",",".") as decimal(17,2)) imp_refinanciado,
sum(cast(REGEXP_REPLACE(c.imp_saldo_cierre, ",",".") as decimal(17,2))) over(partition by c.nro_propuesta , p.categoria_particulares) as imp_agrupacion_producto,
sum(cast(REGEXP_REPLACE(c.imp_saldo_cierre, ",",".") as decimal(17,2))) over(partition by c.nro_propuesta) as imp_total
from bi_corp_staging.relacion_contrato_cru c
left join bi_corp_staging.risksql5_productos p on trim(p.codigo_producto) = trim(c.cod_producto_rel) and trim(p.codigo_subproducto) = trim(c.cod_subprodu_rel) and p.fecha_informacion = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_MORA_PrimasDistribuidas-Daily') }}'
where c.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_MORA_PrimasDistribuidas-Daily') }}'
and cod_tipo_rel = 'H'
;

CREATE TEMPORARY TABLE cru_pivote AS
SELECT
nup,
dor_sucursal,
dor_cuenta,
dor_producto,
dor_subprodu,
dor_divisa,
p.descripcion,
p.categoria_particulares,
p.categoria_carterizados,
p.refinanciacion,
imp_total,
SUM(CASE agrupacion_producto WHEN 'HIPOTECARIO' THEN porc_deuda ELSE 0 END) over(partition by nup, dor_sucursal, dor_cuenta, dor_producto, dor_subprodu, dor_divisa) AS porc_prestamo_hipotecario,
SUM(CASE agrupacion_producto WHEN 'PRENDARIO' THEN porc_deuda ELSE 0 END) over(partition by nup, dor_sucursal, dor_cuenta, dor_producto, dor_subprodu, dor_divisa) AS porc_prestamo_prendario,
SUM(CASE agrupacion_producto WHEN 'TARJETAS' THEN porc_deuda ELSE 0 END) over(partition by nup, dor_sucursal, dor_cuenta, dor_producto, dor_subprodu, dor_divisa) AS porc_tarjetas,
SUM(CASE WHEN agrupacion_producto in ('CUENTA CORRIENTE', 'PLAZO FIJO') THEN porc_deuda ELSE 0 END) over(partition by nup, dor_sucursal, dor_cuenta, dor_producto, dor_subprodu, dor_divisa) AS porc_cuenta_corriente,
SUM(CASE agrupacion_producto WHEN 'PERSONALES' THEN porc_deuda ELSE 0 END) over(partition by nup, dor_sucursal, dor_cuenta, dor_producto, dor_subprodu, dor_divisa) AS porc_personales,
SUM(CASE agrupacion_producto WHEN 'OTROS' THEN porc_deuda ELSE 0 END) over(partition by nup, dor_sucursal, dor_cuenta, dor_producto, dor_subprodu, dor_divisa) AS porc_otros,
1 as distribuido_por_cierre_controlado,
0 as distribuido_por_contratos_ref
FROM contratos_cru c
left join bi_corp_staging.risksql5_productos p on trim(p.codigo_producto) = trim(c.dor_producto) and trim(p.codigo_subproducto) = trim(c.dor_subprodu) and p.fecha_informacion = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_MORA_PrimasDistribuidas-Daily') }}'
;

-- STOCK COMPLETO SIN CONTRATOS COVID NI DIST GENERICA

CREATE TEMPORARY TABLE stock_completo_sin_covid AS

SELECT DISTINCT
cast(nup as bigint) as nup,
cast(dor_sucursal as bigint) as cod_sucursal,
cast(dor_cuenta as bigint) as num_cuenta,
dor_producto as cod_producto,
dor_subprodu as cod_subproducto,
dor_divisa as cod_divisa,
descripcion,
categoria_particulares,
categoria_carterizados,
refinanciacion,
tipo_reestructuracion,
imp_total,
porc_cuenta_corriente as redistribuido_cuenta_corriente,
porc_tarjetas as redistribuido_tarjetas_credito,
porc_personales as redistribuido_prestamos_personales,
porc_otros as redistribuido_otros,
porc_prestamo_hipotecario as redistribuido_hipotecarios,
porc_prestamo_prendario as redistribuido_prendarios,
distribuido_por_cierre_controlado,
distribuido_por_contratos_ref,
0 as distribucion_generica
FROM ref_pivote

UNION ALL

SELECT DISTINCT
cast(nup as bigint) as nup,
cast(dor_sucursal as bigint) as cod_sucursal,
cast(dor_cuenta as bigint) as num_cuenta,
dor_producto as cod_producto,
dor_subprodu as cod_subproducto,
dor_divisa as cod_divisa,
descripcion,
categoria_particulares,
categoria_carterizados,
refinanciacion,
tipo_reestructuracion,
imp_total,
porc_cuenta_corriente as redistribuido_cuenta_corriente,
porc_tarjetas as redistribuido_tarjetas_credito,
porc_personales as redistribuido_prestamos_personales,
porc_otros as redistribuido_otros,
porc_prestamo_hipotecario as redistribuido_hipotecarios,
porc_prestamo_prendario as redistribuido_prendarios,
distribuido_por_cierre_controlado,
distribuido_por_contratos_ref,
0 as distribucion_generica
FROM ref_mora_pivote

UNION ALL

SELECT DISTINCT
cast(nup as bigint) as nup,
cast(dor_sucursal as bigint) as cod_sucursal,
cast(dor_cuenta as bigint) as num_cuenta,
dor_producto as cod_producto,
dor_subprodu as cod_subproducto,
dor_divisa as cod_divisa,
descripcion,
categoria_particulares,
categoria_carterizados,
refinanciacion,
null as tipo_reestructuracion,
imp_total,
porc_cuenta_corriente as redistribuido_cuenta_corriente,
porc_tarjetas as redistribuido_tarjetas_credito,
porc_personales as redistribuido_prestamos_personales,
porc_otros as redistribuido_otros,
porc_prestamo_hipotecario as redistribuido_hipotecarios,
porc_prestamo_prendario as redistribuido_prendarios,
distribuido_por_cierre_controlado,
distribuido_por_contratos_ref,
0 as distribucion_generica
FROM cru_pivote
;

CREATE TEMPORARY TABLE pedt008 AS
select p.pecodofi, p.penumcon, p.pecodpro, p.pecodsub, p.penumper
from (
select pecodofi, penumcon, pecodpro, pecodsub, penumper, ROW_NUMBER() OVER (PARTITION BY pecodofi, penumcon, pecodpro, pecodsub ORDER BY coalesce(pefecbrb,'9999-12-31') DESC, peordpar ASC) AS min_firmante
from bi_corp_staging.malpe_ptb_pedt008
where partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='max_partition_malpe_ptb_pedt008', dag_id='LOAD_CMN_MORA_PrimasDistribuidas-Daily') }}'
AND pecalpar = 'TI'
AND pecodpro IN ('35', '36', '37', '38', '39', '71', '74')
     ) p
where p.min_firmante = 1
;

-- CONTRATOS COVID

CREATE TEMPORARY TABLE contratos_covid_mae AS
select
cast(mae.OFICINA as int) as cod_suc_sucursal,
cast(mae.CUENTA as bigint) as cod_nro_cuenta,
cast(pedt.penumper as int) as cod_per_nup,
mae.PRODUCTO as cod_prod_producto,
mae.SUBPRO as cod_prod_subproducto,
mae.DIVISA as cod_div_divisa,
cast(case when mae.NUM_PROPUES like 'COV%' then SUBSTRING(mae.NUM_PROPUES, 4, 3) else null end as int) as cod_suc_sucursaloriginante,
cast(case when mae.NUM_PROPUES like 'COV%' then SUBSTRING(mae.NUM_PROPUES, 7, 11) else null end as bigint) as cod_pre_cuentaoriginante,
case when mae.NUM_PROPUES like 'COV%' then 1 else 0 end as flag_pre_marcacovid
from bi_corp_staging.malug_ugdtmae mae
left join pedt008 pedt on  mae.oficina =pedt.pecodofi and mae.cuenta = pedt.penumcon and mae.producto = pedt.pecodpro and mae.subpro = pedt.pecodsub
where mae.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_MORA_PrimasDistribuidas-Daily') }}'
and mae.NUM_PROPUES like 'COV%'
and mae.sitpres in ('0','3','P');
;

CREATE TEMPORARY TABLE alta_nuevos_covid AS

select distinct
cast(sin.nup as bigint) as nup,
cast(covid.cod_suc_sucursal as bigint) as cod_sucursal,
cast(covid.cod_nro_cuenta as bigint) as num_cuenta,
covid.cod_prod_producto as cod_producto,
covid.cod_prod_subproducto as cod_subproducto,
covid.cod_div_divisa as cod_divisa,
p.descripcion,
sin.categoria_particulares,
sin.categoria_carterizados,
sin.refinanciacion,
sin.tipo_reestructuracion,
sin.imp_total,
sin.redistribuido_cuenta_corriente,
sin.redistribuido_tarjetas_credito,
sin.redistribuido_prestamos_personales,
sin.redistribuido_otros,
sin.redistribuido_hipotecarios,
sin.redistribuido_prendarios,
sin.distribuido_por_cierre_controlado,
sin.distribuido_por_contratos_ref,
sin.distribucion_generica
FROM stock_completo_sin_covid sin
INNER JOIN contratos_covid_mae covid ON covid.cod_pre_cuentaoriginante = cast(sin.num_cuenta as bigint) AND covid.cod_suc_sucursaloriginante = cast(sin.cod_sucursal as int) and covid.cod_per_nup = cast(sin.nup as int) AND flag_pre_marcacovid = 1
left join bi_corp_staging.risksql5_productos p on trim(p.codigo_producto) = trim(covid.cod_prod_producto) and trim(p.codigo_subproducto) = trim(covid.cod_prod_subproducto) and p.fecha_informacion = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_MORA_PrimasDistribuidas-Daily') }}'
;

CREATE TEMPORARY TABLE alta_nuevos_covid_mora AS
SELECT
c.nup,
c.cod_sucursal,
c.num_cuenta,
p.codigo_producto_mora as cod_producto,
p.codigo_subproducto_mora as cod_subproducto,
c.cod_divisa,
p.descripcion,
c.categoria_particulares,
c.categoria_carterizados,
c.refinanciacion,
c.tipo_reestructuracion,
c.imp_total,
c.redistribuido_cuenta_corriente,
c.redistribuido_tarjetas_credito,
c.redistribuido_prestamos_personales,
c.redistribuido_otros,
c.redistribuido_hipotecarios,
c.redistribuido_prendarios,
c.distribuido_por_cierre_controlado,
c.distribuido_por_contratos_ref,
c.distribucion_generica
FROM alta_nuevos_covid c
left join bi_corp_staging.risksql5_productos p on trim(p.codigo_producto) = trim(c.cod_producto) and trim(p.codigo_subproducto) = trim(c.cod_subproducto) and p.fecha_informacion = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_MORA_PrimasDistribuidas-Daily') }}'
;

CREATE TEMPORARY TABLE contratos_covid AS
SELECT x.*
FROM (
SELECT * FROM alta_nuevos_covid
UNION ALL
SELECT * FROM alta_nuevos_covid_mora
) x
;

-- STOCK COMPLETO CON CONTRATOS COVID

CREATE TEMPORARY TABLE stock_completo_con_covid AS
SELECT x.*
FROM (
SELECT * FROM stock_completo_sin_covid
UNION ALL
SELECT * FROM contratos_covid
) x
;

-- CONTRATOS CON DISTRIBUCION GENERICA

CREATE TEMPORARY TABLE distribucion_generica AS
SELECT x.*
FROM (
-- cierres controlados que van a tener dist generica
select
w.waguxdex_num_persona as nup,
w.waguxdex_cod_centro as cod_sucursal,
w.waguxdex_num_contrato as num_cuenta,
w.waguxdex_cod_producto as cod_producto,
w.waguxdex_cod_subprodu as cod_subproducto,
w.waguxdex_cod_divisa as cod_divisa,
p.descripcion,
p.categoria_particulares,
p.categoria_carterizados,
p.refinanciacion,
p.tipo_reestructuracion,
null as imp_total,
g.porc_cuenta_cte as redistribuido_cuenta_corriente,
g.porc_tarjetas as redistribuido_tarjetas_credito,
g.porc_personales as redistribuido_prestamos_personales,
g.porc_otros as redistribuido_otros,
g.porc_hipotecario as redistribuido_hipotecarios,
g.porc_prendario as redistribuido_prendarios,
0 as distribuido_por_cierre_controlado,
0 as distribuido_por_contratos_ref,
1 as distribucion_generica
from bi_corp_staging.garra_wagucdex w
left join bi_corp_staging.risksql5_productos p on trim(p.codigo_producto) = trim(w.waguxdex_cod_producto) and trim(p.codigo_subproducto) = trim(w.waguxdex_cod_subprodu) and p.fecha_informacion = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_MORA_PrimasDistribuidas-Daily') }}'
left join bi_corp_staging.relacion_contrato_cru c on c.num_persona = w.waguxdex_num_persona and c.num_contrato = w.waguxdex_num_contrato  and c.cod_centro = w.waguxdex_cod_centro  and c.cod_producto = w.waguxdex_cod_producto and c.cod_subprodu = w.waguxdex_cod_subprodu and c.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_MORA_PrimasDistribuidas-Daily') }}'
left join bi_corp_staging.moria_contratos_md m on w.waguxdex_cod_centro = m.mdec1610_cod_centro and w.waguxdex_num_contrato = m.mdec1610_num_contrato and w.waguxdex_cod_producto = m.mdec1610_cod_producto and w.waguxdex_cod_subprodu = m.mdec1610_cod_subprodu and w.waguxdex_cod_divisa = m.mdec1610_cod_divisa and m.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_MORA_PrimasDistribuidas-Daily') }}'
left join bi_corp_common.dim_mora_primas_genericas g on g.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_MORA_PrimasDistribuidas-Daily') }}' and g.periodo = regexp_replace(substr(m.mdec1610_fec_ingreso, 1, 7), "-", "") and g.distribucion = 'c'
where w.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_MORA_PrimasDistribuidas-Daily') }}'
and (waguxdex_cod_producto = '70' and waguxdex_cod_subprodu = 'C070')
and c.num_contrato is null

UNION ALL

-- perimetro de refinanciaciones, producto 71 que van a tener dist generica por no estar en maestro de reestructuraciones
select
w.waguxdex_num_persona as nup,
w.waguxdex_cod_centro as cod_sucursal,
w.waguxdex_num_contrato as num_cuenta,
w.waguxdex_cod_producto as cod_producto,
w.waguxdex_cod_subprodu as cod_subproducto,
w.waguxdex_cod_divisa as cod_divisa,
p.descripcion,
p.categoria_particulares,
p.categoria_carterizados,
p.refinanciacion,
p.tipo_reestructuracion,
null as imp_total,
g.porc_cuenta_cte as redistribuido_cuenta_corriente,
g.porc_tarjetas as redistribuido_tarjetas_credito,
g.porc_personales as redistribuido_prestamos_personales,
g.porc_otros as redistribuido_otros,
g.porc_hipotecario as redistribuido_hipotecarios,
g.porc_prendario as redistribuido_prendarios,
0 as distribuido_por_cierre_controlado,
0 as distribuido_por_contratos_ref,
1 as distribucion_generica
from bi_corp_staging.garra_wagucdex w
left join stock_completo_con_covid r on r.nup  = cast(w.waguxdex_num_persona as bigint)
and r.num_cuenta = cast(w.waguxdex_num_contrato as bigint)
and r.cod_sucursal = cast(w.waguxdex_cod_centro as bigint)
and r.cod_producto = w.waguxdex_cod_producto
and r.cod_subproducto = w.waguxdex_cod_subprodu
left join bi_corp_staging.risksql5_productos p on trim(p.codigo_producto) = trim(w.waguxdex_cod_producto) and trim(p.codigo_subproducto) = trim(w.waguxdex_cod_subprodu) and p.fecha_informacion = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_MORA_PrimasDistribuidas-Daily') }}'
left join bi_corp_common.dim_mora_primas_genericas g on g.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_MORA_PrimasDistribuidas-Daily') }}' and g.periodo = regexp_replace(substr(w.waguxdex_fec_alt_prod , 1, 7), "-", "") and g.distribucion = 'c'
where w.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_MORA_PrimasDistribuidas-Daily') }}'
and waguxdex_cod_producto = '71'
and r.num_cuenta is null

UNION ALL

-- perimetro de productos 71 en mora (actualmente producto 70): prima generica. ref
select
w.waguxdex_num_persona as nup,
w.waguxdex_cod_centro as cod_sucursal,
w.waguxdex_num_contrato as num_cuenta,
w.waguxdex_cod_producto as cod_producto,
w.waguxdex_cod_subprodu as cod_subproducto,
w.waguxdex_cod_divisa as cod_divisa,
p.descripcion,
p.categoria_particulares,
p.categoria_carterizados,
p.refinanciacion,
p.tipo_reestructuracion,
null as imp_total,
g.porc_cuenta_cte as redistribuido_cuenta_corriente,
g.porc_tarjetas as redistribuido_tarjetas_credito,
g.porc_personales as redistribuido_prestamos_personales,
g.porc_otros as redistribuido_otros,
g.porc_hipotecario as redistribuido_hipotecarios,
g.porc_prendario as redistribuido_prendarios,
0 as distribuido_por_cierre_controlado,
0 as distribuido_por_contratos_ref,
1 as distribucion_generica
from bi_corp_staging.garra_wagucdex w
left join bi_corp_staging.risksql5_productos p on trim(p.codigo_producto_mora) = trim(w.waguxdex_cod_producto) and trim(p.codigo_subproducto_mora) = trim(w.waguxdex_cod_subprodu) and p.fecha_informacion = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_MORA_PrimasDistribuidas-Daily') }}'
left join stock_completo_con_covid r on r.nup  = cast(w.waguxdex_num_persona as bigint)
and r.num_cuenta = cast(w.waguxdex_num_contrato as bigint)
and r.cod_sucursal = cast(w.waguxdex_cod_centro as bigint)
and r.cod_producto = w.waguxdex_cod_producto
and r.cod_subproducto = w.waguxdex_cod_subprodu
left join bi_corp_common.dim_mora_primas_genericas g on g.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_MORA_PrimasDistribuidas-Daily') }}' and g.periodo = regexp_replace(substr(w.waguxdex_fec_alt_prod , 1, 7), "-", "") and g.distribucion = 'c'
where w.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_MORA_PrimasDistribuidas-Daily') }}'
and p.refinanciacion = 'true'
and r.num_cuenta is null
) x
;

-- UNION DE AMBOS

CREATE TEMPORARY TABLE stock_completo AS
SELECT x.*
FROM (
SELECT distinct * FROM stock_completo_con_covid
UNION ALL
SELECT distinct * FROM distribucion_generica
) x
;

-- INSERTO AGREGANDO 0 A LOS CASOS QUE NO TIENEN DISTRIBUCION

INSERT OVERWRITE TABLE bi_corp_common.stk_mora_primas_redistribuidas
PARTITION (partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_MORA_PrimasDistribuidas-Daily') }}')

SELECT DISTINCT
c.nup,
c.cod_sucursal,
c.num_cuenta,
c.cod_producto,
c.cod_subproducto,
c.cod_divisa,
c.descripcion,
c.categoria_particulares,
c.categoria_carterizados,
c.refinanciacion,
c.tipo_reestructuracion,
c.imp_total,
cast(COALESCE(c.redistribuido_cuenta_corriente, 0) as string) AS redistribuido_cuenta_corriente,
cast(COALESCE(c.redistribuido_tarjetas_credito, 0) as string) AS redistribuido_tarjetas_credito,
cast(COALESCE(c.redistribuido_prestamos_personales, 0) as string) AS redistribuido_prestamos_personales,
cast(COALESCE(c.redistribuido_otros, 0) as string) AS redistribuido_otros,
cast(COALESCE(c.redistribuido_hipotecarios, 0) as string) AS redistribuido_hipotecarios,
cast(COALESCE(c.redistribuido_prendarios, 0) as string) AS redistribuido_prendarios,
c.distribuido_por_cierre_controlado,
c.distribuido_por_contratos_ref,
c.distribucion_generica
FROM stock_completo c
;
