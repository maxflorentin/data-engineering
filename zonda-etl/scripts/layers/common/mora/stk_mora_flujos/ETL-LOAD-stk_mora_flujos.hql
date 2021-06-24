set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=4000000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.exec.dynamic.partition.mode=nonstrict;
set mapred.job.queue.name=root.dataeng;

CREATE TEMPORARY TABLE flujo_mora_mes_anterior AS
select
periodo,
clave,
cast(nup as bigint) as nup,
cast(cod_sucursal as bigint) as cod_sucursal,
cast(num_cuenta as bigint) as num_cuenta,
cast(nro_ingreso as int) as nro_ingreso,
salida_mora_anterior,
ingreso_mora,
salida_mora,
cast(meses_fuera_de_mora_hasta_ingresar as int) as meses_fuera_de_mora_hasta_ingresar,
cast(meses_en_mora_hasta_salir as int) as meses_en_mora_hasta_salir,
ingreso_despues_de_12_meses_fuera_de_mora,
inexistente_periodo_actual,
ingreso_mora_lgd,
cast(meses_en_mora_lgd as int) as meses_en_mora_lgd,
null as  ultimo_dia_del_periodo,
cast(pagos_consecutivos as int) as pagos_consecutivos ,
0 as marca_reingreso,
1 as actualizado ,
0 as contrato_migrado
from bi_corp_common.stk_mora_flujo_permanencia
where partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='previous_month_last_working_day', dag_id='LOAD_CMN_MORA-Daily') }}'
and periodo = '{{ ti.xcom_pull(task_ids='InputConfig', key='previous_month', dag_id='LOAD_CMN_MORA-Daily') }}'
;

CREATE TEMPORARY TABLE cierre_suc_bimon as
select distinct substring(waguctra_txt_relleno, 4, 8) as nup, waguctra_cod_centro as centro_or, waguctra_num_contrato as contrato_or, waguctra_cod_centrod as centro_de, waguctra_num_contratd as contrato_de, waguctra_fec_traspaso as fec_traspaso
from  bi_corp_staging.garra_rel_contrato_integ_suc
where partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='max_partition_garra_rel_contrato_integ_suc', dag_id='LOAD_CMN_MORA-Daily') }}'
and substr(waguctra_fec_traspaso, 1, 7) = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_MORA-Daily') }}'
;

CREATE TEMPORARY TABLE pedt008 AS
select p.pecodofi, p.penumcon, p.pecodpro, p.pecodsub, p.penumper
from bi_corp_staging.malpe_ptb_pedt008 p
inner join
(select pecodofi, penumcon, pecodpro, pecodsub, min(cast(case when REGEXP_REPLACE(PEORDPAR, "^0+", '')='' then '0' else REGEXP_REPLACE(PEORDPAR, "^0+", '') end as int)) PEORDPAR
from bi_corp_staging.malpe_ptb_pedt008
where partition_date= '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_MORA-Daily') }}'
AND pecalpar = 'TI'
AND pecodpro IN ('35', '36', '37', '38', '39', '71', '74')
group by pecodofi, penumcon, pecodpro, pecodsub) t2
on t2.pecodofi=p.pecodofi and t2.penumcon=p.penumcon and t2.pecodpro=p.pecodpro and t2.pecodsub=p.pecodsub and t2.peordpar=cast(case when REGEXP_REPLACE(p.PEORDPAR, "^0+", '')='' then '0' else REGEXP_REPLACE(p.PEORDPAR, "^0+", '') end as int)
where p.partition_date= '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_MORA-Daily') }}'
AND p.pecalpar = 'TI'
;

CREATE TEMPORARY TABLE contratos_covid_pre AS
select
mae.ENTIDAD as cod_pre_entidad,
mae.OFICINA as cod_suc_sucursal,
mae.CUENTA as contrato,
pedt.penumper as nup,
concat(pedt.penumper, mae.CUENTA) as clave,
mae.PRODUCTO as cod_pre_producto,
mae.SUBPRO as cod_pre_subproducto,
mae.NUM_PROPUES,
cast(lpad(substr(mae.num_propues, 7, 11), 12 ,'0') as bigint) as contrato_origen
from bi_corp_staging.malug_ugdtmae mae
left join pedt008 pedt on  mae.oficina =pedt.pecodofi and mae.cuenta = pedt.penumcon and mae.producto = pedt.pecodpro and mae.subpro = pedt.pecodsub
where mae.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_MORA-Daily') }}'
and mae.NUM_PROPUES like 'COV%'
;

CREATE TEMPORARY TABLE trazabilidad AS
select s.nup as nup, s.contrato_or as cierre_contrato_origen, s.contrato_de as cierre_contrato_destino, c.contrato_origen as prestamo_contrato_origen, c.contrato as prestamo_contrato_destino, s.centro_de as cierre_sucursal_destino, s.centro_or as cierre_sucursal_origen
from cierre_suc_bimon s
inner join contratos_covid_pre c on s.nup = c.nup and s.contrato_de = c.contrato and s.centro_de = c.cod_suc_sucursal
;

CREATE TEMPORARY TABLE contratos_covid AS
select distinct
c.cod_pre_entidad,
cast(c.cod_suc_sucursal as bigint) as cod_suc_sucursal,
cast(c.contrato as bigint) as contrato,
cast(c.nup as bigint) as nup,
c.clave,
c.cod_pre_producto,
c.cod_pre_subproducto,
c.num_propues,
coalesce(cast(x.cierre_contrato_origen as bigint), c.contrato_origen) as contrato_origen
from contratos_covid_pre c
left join trazabilidad x on c.nup = x.nup and c.contrato = x.cierre_contrato_destino and c.cod_suc_sucursal = x.cierre_sucursal_destino
;

CREATE TEMPORARY TABLE flujo_mora_mes_anterior_cierre AS

select distinct
c.periodo,
concat(lpad(cast(c.nup as string), 8, '0'), lpad(cast(r.contrato_de as string), 12, '0')) as clave,
c.nup,
null as cod_sucursal,
cast(r.contrato_de as bigint) as num_cuenta,
c.nro_ingreso,
c.salida_mora_anterior,
c.ingreso_mora,
c.salida_mora,
c.meses_fuera_de_mora_hasta_ingresar,
c.meses_en_mora_hasta_salir,
c.ingreso_despues_de_12_meses_fuera_de_mora,
c.inexistente_periodo_actual,
c.ingreso_mora_lgd,
c.meses_en_mora_lgd,
c.ultimo_dia_del_periodo,
c.pagos_consecutivos,
c.marca_reingreso,
c.actualizado ,
c.contrato_migrado
from flujo_mora_mes_anterior c
inner join cierre_suc_bimon r on cast(r.contrato_or as bigint) = c.num_cuenta and cast(r.nup as bigint) = c.nup
union all
select c.*
from flujo_mora_mes_anterior c
left join cierre_suc_bimon r on cast(r.contrato_or as bigint) = c.num_cuenta and cast(r.nup as bigint) = c.nup
where r.contrato_or is null
;

CREATE TEMPORARY TABLE flujo_mora_mes_anterior_cierre_covid as

select *
from flujo_mora_mes_anterior_cierre

union all

select
periodo,
covid.clave,
covid.nup,
t.cod_sucursal,
covid.contrato as num_cuenta,
nro_ingreso,
salida_mora_anterior,
ingreso_mora,
salida_mora,
meses_fuera_de_mora_hasta_ingresar,
meses_en_mora_hasta_salir,
ingreso_despues_de_12_meses_fuera_de_mora,
inexistente_periodo_actual,
ingreso_mora_lgd,
meses_en_mora_lgd,
null as  ultimo_dia_del_periodo,
pagos_consecutivos,
0 as marca_reingreso,
1 as actualizado ,
0 as contrato_migrado
from  flujo_mora_mes_anterior_cierre t
inner join contratos_covid covid on covid.nup = t.nup and covid.contrato_origen = t.num_cuenta
where covid.clave not in (select distinct clave from flujo_mora_mes_anterior) and covid.nup is not null
;

CREATE TEMPORARY TABLE mora_periodo_actual AS
select
m.periodo, m.nup, null as cod_sucursal, m.num_cuenta,
concat(lpad(cast(m.nup as string), 8, '0'), lpad(cast(m.num_cuenta as string), 12, '0')) as clave,
CASE WHEN substr(m.fecha_alta_producto, 1, 4) ='9999' THEN '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_MORA-Daily') }}' ELSE m.fecha_alta_producto END as fecha_alta_producto,
m.importe_riesgo,
0 as contrato_migrado
from bi_corp_common.stk_mora_404 m
inner join (
select periodo, nup, num_cuenta, cod_sucursal, min(fecha_alta_producto) as fecha_alta_producto, min(fecha_situacion_irregular) as fecha_situacion_irregular, sum(importe_riesgo) as importe_riesgo
from bi_corp_common.stk_mora_404
where partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_MORA-Daily') }}'
group by periodo, nup, num_cuenta, cod_sucursal
) x
on m.nup = x.nup and m.num_cuenta = x.num_cuenta and m.fecha_alta_producto = x.fecha_alta_producto
where m.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_MORA-Daily') }}'
group by m.periodo, m.nup, m.num_cuenta, m.fecha_alta_producto, m.fecha_situacion_irregular, m.importe_riesgo
;


CREATE TEMPORARY TABLE flujo_mora_mes_actual AS

-- contratos que estan en mora actual pero no en el flujo historico
select
'{{ ti.xcom_pull(task_ids='InputConfig', key='month', dag_id='LOAD_CMN_MORA-Daily') }}' as periodo,
m.clave as clave,
m.nup as nup,
m.cod_sucursal as cod_sucursal,
m.num_cuenta as num_cuenta,
1 as nro_ingreso,
null as salida_mora_anterior,
'{{ ti.xcom_pull(task_ids='InputConfig', key='last_calendar_day', dag_id='LOAD_CMN_MORA-Daily') }}' as ingreso_mora,  -- ultimo dia calendario del mes
'{{ ti.xcom_pull(task_ids='InputConfig', key='last_calendar_day', dag_id='LOAD_CMN_MORA-Daily') }}' as salida_mora,  -- ultimo dia calendario del mes
CASE WHEN substr(m.fecha_alta_producto, 1, 4) <> '9999' THEN CAST(months_between('{{ ti.xcom_pull(task_ids='InputConfig', key='last_calendar_day', dag_id='LOAD_CMN_MORA-Daily') }}', m.fecha_alta_producto) as int) ELSE 1 END as meses_fuera_de_mora_hasta_ingresar,  -- ultimo dia calendario del mes
1 as meses_en_mora_hasta_salir,
1 as ingreso_despues_de_12_meses_fuera_de_mora,
0 as inexistente_periodo_actual,
'{{ ti.xcom_pull(task_ids='InputConfig', key='last_calendar_day', dag_id='LOAD_CMN_MORA-Daily') }}' as ingreso_mora_lgd, -- ultimo dia calendario del mes
1 as meses_en_mora_lgd,
'{{ ti.xcom_pull(task_ids='InputConfig', key='last_calendar_day', dag_id='LOAD_CMN_MORA-Daily') }}' as ultimo_dia_del_periodo, -- ultimo dia calendario del mes
0 as pagos_consecutivos,
0 as marca_reingreso,
1 as actualizado ,
m.contrato_migrado as contrato_migrado
from mora_periodo_actual m --404
left join flujo_mora_mes_anterior_cierre_covid i on m.clave = i.clave -- flujo
where i.clave is NULL

UNION ALL

-- contratos que estan en el flujo historico y en mora actual (reingreso)
select
'{{ ti.xcom_pull(task_ids='InputConfig', key='month', dag_id='LOAD_CMN_MORA-Daily') }}' as periodo,
m.clave as clave,
m.nup as nup,
m.cod_sucursal as cod_sucursal,
m.num_cuenta as num_cuenta,
m.nro_ingreso + 1 as nro_ingreso,
m.salida_mora as salida_mora_anterior,
'{{ ti.xcom_pull(task_ids='InputConfig', key='last_calendar_day', dag_id='LOAD_CMN_MORA-Daily') }}' as ingreso_mora, -- ultimo dia calendario del mes
'{{ ti.xcom_pull(task_ids='InputConfig', key='last_calendar_day', dag_id='LOAD_CMN_MORA-Daily') }}' as salida_mora,  -- ultimo dia calendario del mes
cast(m.meses_fuera_de_mora_hasta_ingresar as int) as meses_fuera_de_mora_hasta_ingresar,  -- AGREGAR UN MES
1 as meses_en_mora_hasta_salir,
case when cast(months_between('{{ ti.xcom_pull(task_ids='InputConfig', key='last_calendar_day', dag_id='LOAD_CMN_MORA-Daily') }}', m.salida_mora_anterior) as int) > 12  then 1 else 0 end as ingreso_despues_de_12_meses_fuera_de_mora, -- nuevo ingreso a mora vs salida mora anterior calcular meses
0 as inexistente_periodo_actual,
case when cast(months_between('{{ ti.xcom_pull(task_ids='InputConfig', key='last_calendar_day', dag_id='LOAD_CMN_MORA-Daily') }}', m.salida_mora_anterior) as int) > 12 then '{{ ti.xcom_pull(task_ids='InputConfig', key='last_calendar_day', dag_id='LOAD_CMN_MORA-Daily') }}' else m.ingreso_mora_lgd end as ingreso_mora_lgd,
case when cast(months_between('{{ ti.xcom_pull(task_ids='InputConfig', key='last_calendar_day', dag_id='LOAD_CMN_MORA-Daily') }}', m.salida_mora_anterior) as int) > 12 then 1 else m.meses_en_mora_lgd + 1 end as meses_en_mora_lgd,
'{{ ti.xcom_pull(task_ids='InputConfig', key='last_calendar_day', dag_id='LOAD_CMN_MORA-Daily') }}' as ultimo_dia_del_periodo, -- ultimo dia calendario del mes
m.pagos_consecutivos as pagos_consecutivos,
1 as marca_reingreso,
1 as actualizado,
m.contrato_migrado as contrato_migrado
from flujo_mora_mes_anterior_cierre_covid m -- flujo
inner join mora_periodo_actual i on m.clave = i.clave -- 404
where m.inexistente_periodo_actual = 1

UNION ALL

-- contratos que estan en el flujo historico y en mora actual (continua)
select
'{{ ti.xcom_pull(task_ids='InputConfig', key='month', dag_id='LOAD_CMN_MORA-Daily') }}' as periodo,
m.clave as clave,
m.nup as nup,
m.cod_sucursal as cod_sucursal,
m.num_cuenta as num_cuenta,
m.nro_ingreso as nro_ingreso,
m.salida_mora_anterior as salida_mora_anterior,
m.ingreso_mora as ingreso_mora,
'{{ ti.xcom_pull(task_ids='InputConfig', key='last_calendar_day', dag_id='LOAD_CMN_MORA-Daily') }}' as salida_mora,  -- ultimo dia calendario del mes
m.meses_fuera_de_mora_hasta_ingresar as meses_fuera_de_mora_hasta_ingresar,  -- AGREGAR UN MES
m.meses_en_mora_hasta_salir + 1 as meses_en_mora_hasta_salir,
0 as ingreso_despues_de_12_meses_fuera_de_mora, -- nuevo ingreso a mora vs salida mora anterior calcular meses
0 as inexistente_periodo_actual,
m.ingreso_mora_lgd as ingreso_mora_lgd,
m.meses_en_mora_lgd + 1 as meses_en_mora_lgd,
'{{ ti.xcom_pull(task_ids='InputConfig', key='last_calendar_day', dag_id='LOAD_CMN_MORA-Daily') }}' as ultimo_dia_del_periodo, -- ultimo dia calendario del mes
m.pagos_consecutivos as pagos_consecutivos,
0 as marca_reingreso,
1 as actualizado,
m.contrato_migrado as contrato_migrado
from flujo_mora_mes_anterior_cierre_covid m -- flujo
inner join mora_periodo_actual i on m.clave = i.clave -- 404
where m.inexistente_periodo_actual = 0

UNION ALL

-- contratos que estan en el flujo historico y NO en mora actual (Mes anterior en Mora)
select
'{{ ti.xcom_pull(task_ids='InputConfig', key='month', dag_id='LOAD_CMN_MORA-Daily') }}' as periodo,
m.clave as clave,
m.nup as nup,
m.cod_sucursal as cod_sucursal,
m.num_cuenta as num_cuenta,
m.nro_ingreso as nro_ingreso,
m.salida_mora_anterior as salida_mora_anterior,
m.ingreso_mora as ingreso_mora,
m.salida_mora as salida_mora,
1 as meses_fuera_de_mora_hasta_ingresar,
m.meses_en_mora_hasta_salir as meses_en_mora_hasta_salir,
0 as ingreso_despues_de_12_meses_fuera_de_mora,
1 as inexistente_periodo_actual,
m.ingreso_mora_lgd as ingreso_mora_lgd,
m.meses_en_mora_lgd + 1 as meses_en_mora_lgd,
'{{ ti.xcom_pull(task_ids='InputConfig', key='last_calendar_day', dag_id='LOAD_CMN_MORA-Daily') }}' as ultimo_dia_del_periodo, -- ultimo dia calendario del mes
m.pagos_consecutivos as pagos_consecutivos,
0 as marca_reingreso,
1 as actualizado,
m.contrato_migrado as contrato_migrado
from flujo_mora_mes_anterior_cierre_covid m -- FLUJO
left join mora_periodo_actual i on m.clave = i.clave --404
where i.clave is NULL and m.inexistente_periodo_actual = 0

UNION ALL

-- contratos que estan en el flujo historico y NO en mora actual (Mes anterior NO en Mora)
select
'{{ ti.xcom_pull(task_ids='InputConfig', key='month', dag_id='LOAD_CMN_MORA-Daily') }}' as periodo,
m.clave as clave,
m.nup as nup,
m.cod_sucursal as cod_sucursal,
m.num_cuenta as num_cuenta,
m.nro_ingreso as nro_ingreso,
m.salida_mora_anterior as salida_mora_anterior,
m.ingreso_mora as ingreso_mora,
m.salida_mora as salida_mora,
m.meses_fuera_de_mora_hasta_ingresar + 1 as meses_fuera_de_mora_hasta_ingresar,
m.meses_en_mora_hasta_salir as meses_en_mora_hasta_salir,
0 as ingreso_despues_de_12_meses_fuera_de_mora,
1 as inexistente_periodo_actual,
case when cast(months_between('{{ ti.xcom_pull(task_ids='InputConfig', key='last_calendar_day', dag_id='LOAD_CMN_MORA-Daily') }}', m.salida_mora_anterior) as int) > 12 then '{{ ti.xcom_pull(task_ids='InputConfig', key='last_calendar_day', dag_id='LOAD_CMN_MORA-Daily') }}' else m.ingreso_mora_lgd end as ingreso_mora_lgd, -- ultimo dia calendario en ambas
case when cast(months_between('{{ ti.xcom_pull(task_ids='InputConfig', key='last_calendar_day', dag_id='LOAD_CMN_MORA-Daily') }}', m.salida_mora_anterior) as int) > 12 then 0 else m.meses_en_mora_lgd + 1 end as meses_en_mora_lgd,
'{{ ti.xcom_pull(task_ids='InputConfig', key='last_calendar_day', dag_id='LOAD_CMN_MORA-Daily') }}' as ultimo_dia_del_periodo, -- ultimo dia calendario del mes
m.pagos_consecutivos as pagos_consecutivos,
0 as marca_reingreso,
1 as actualizado,
m.contrato_migrado as contrato_migrado
from flujo_mora_mes_anterior_cierre_covid m -- FLUJO
left join mora_periodo_actual i on m.clave = i.clave --404
where i.clave is NULL and m.inexistente_periodo_actual = 1
;

INSERT OVERWRITE TABLE bi_corp_common.stk_mora_flujo_permanencia
PARTITION (partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='last_working_day', dag_id='LOAD_CMN_MORA-Daily') }}')

SELECT distinct *
FROM flujo_mora_mes_actual

;