set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=4000000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.exec.dynamic.partition.mode=nonstrict;
set mapred.job.queue.name=root.dataeng;

CREATE TEMPORARY TABLE  aqua as
select distinct num_persona, cast(coef_rating as string) as coef_rating , fec_periodo from santander_business_risk_arda.rating_aqua_nilo
union all
SELECT mdr.alias_nup as num_persona, rating_interno as coef_rating, regexp_replace(substr(aq.partition_date, 1, 7), '-', '') as fec_periodo
FROM bi_corp_staging.aqua_clientes_asoc_geconomicos aq
INNER JOIN
(select * from bi_corp_staging.mdr_contrapartes where partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_MORA-Monthly') }}') mdr
ON (aq.unidad_operativa = mdr.shortname)
where aq.partition_date >= '2020-10-01'
and
aq.pais_unidad_operativa_kgl = 'AR'
and aq.rating_interno != 'SIN_CALIFI'
and mdr.alias_nup != '<NOT FOUND>'
;

CREATE TEMPORARY TABLE sge as
select sge.num_persona, cast((case when sge.coef_rating > 10 then sge.coef_rating / 10 else sge.coef_rating end) as string) as coef_rating, sge.fec_periodo
from ( select distinct num_persona, coef_rating, regexp_replace(substr(fec_periodo, 1, 7), '-', '') as fec_periodo from santander_business_risk_arda.rating_sge ) sge
;

CREATE TEMPORARY TABLE rating_union as
SELECT *
FROM (
select * from aqua
union all
select * from sge
) x
;

CREATE TEMPORARY TABLE rating_min as
select num_persona, coef_rating, waguxdex_fec_alt_prod, ROW_NUMBER () over (partition by num_persona, waguxdex_fec_alt_prod order by diff asc) as rownum
from (
select distinct num_persona, coef_rating, abs(cast(fec_periodo as int) - cast(regexp_replace(substr(w.waguxdex_fec_alt_prod , 1, 7), '-', '') as int)) as diff, w.waguxdex_fec_alt_prod , r.fec_periodo
from bi_corp_staging.garra_wagucdex w
inner join rating_union r on r.num_persona = w.waguxdex_num_persona
where w.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_MORA-Monthly') }}'
and cast(fec_periodo as int) >= cast(regexp_replace(substr(w.waguxdex_fec_alt_prod , 1, 7), '-', '') as int)
) x
;

CREATE TEMPORARY TABLE rating as
select *
from rating_min
where rownum = 1
;

CREATE TEMPORARY TABLE deuda_adsf as
select a.cod_per_nup, a.cod_cys_entidad, a.cod_suc_sucursal, a.cod_nro_cuenta, a.cod_prod_producto, a.cod_prod_subproducto, a.cod_div_divisa, sum(fc_cys_importedeuda) as imp_deuda
from bi_corp_common.stk_cys_adsf a
LEFT JOIN bi_corp_common.stk_per_personas pe on pe.cod_per_nup = a.cod_per_nup and pe.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_MORA-Monthly') }}'
LEFT JOIN bi_corp_staging.risksql5_segmentos s ON trim(pe.cod_per_segmentoduro) = trim(s.ramo) and s.fecha_informacion = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_MORA-Monthly') }}'
LEFT JOIN bi_corp_staging.risksql5_productos p ON a.cod_prod_producto = cast(p.codigo_producto as bigint) and trim(a.cod_prod_subproducto) = trim(p.codigo_subproducto)  and p.fecha_informacion = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_MORA-Monthly') }}'
where a.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='last_calendar_day', dag_id='LOAD_CMN_MORA-Monthly') }}'
and trim(cod_cys_castigocontrato) = 'N'
group by a.cod_per_nup, a.cod_cys_entidad, a.cod_suc_sucursal, a.cod_nro_cuenta, a.cod_prod_producto, a.cod_prod_subproducto, a.cod_div_divisa
;

CREATE TEMPORARY TABLE deuda_cliente as
select waguxdex_num_persona as nup,
SUM(CASE
WHEN trim(s.segmento_control) = 'INDIVIDUOS' and p.categoria_particulares = 'TARJETAS' THEN a.imp_deuda
WHEN trim(s.segmento_control) != 'INDIVIDUOS' and p.categoria_carterizados = 'TARJETAS' THEN a.imp_deuda
ELSE COALESCE(w.waguxdex_imp_riesmolo ,0) END
) as deuda_cliente
from bi_corp_staging.garra_wagucdex w
LEFT JOIN bi_corp_common.stk_per_personas pe on pe.cod_per_nup = cast(trim(w.waguxdex_num_persona) as bigint) and pe.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_MORA-Monthly') }}'
LEFT JOIN bi_corp_staging.risksql5_segmentos s ON trim(pe.cod_per_segmentoduro) = trim(s.ramo) and s.fecha_informacion = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_MORA-Monthly') }}'
LEFT JOIN bi_corp_staging.risksql5_productos p ON w.waguxdex_cod_producto = p.codigo_producto and trim(w.waguxdex_cod_subprodu) = trim(p.codigo_subproducto)  and p.fecha_informacion = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_MORA-Monthly') }}'
LEFT JOIN deuda_adsf a ON cast(w.waguxdex_num_persona as bigint) = a.cod_per_nup and cast(w.waguxdex_num_contrato as bigint) = a.cod_nro_cuenta and cast(w.waguxdex_cod_centro as bigint) = a.cod_suc_sucursal and cast(w.waguxdex_cod_producto as bigint) = a.cod_prod_producto and w.waguxdex_cod_subprodu = a.cod_prod_subproducto and w.waguxdex_cod_divisa = a.cod_div_divisa
where w.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_MORA-Monthly') }}'
group by waguxdex_num_persona
;

CREATE TEMPORARY TABLE deuda_morosa as
select waguxdex_num_persona as nup,
SUM(CASE
WHEN trim(s.segmento_control) = 'INDIVIDUOS' and p.categoria_particulares = 'TARJETAS' THEN a.imp_deuda
WHEN trim(s.segmento_control) != 'INDIVIDUOS' and p.categoria_carterizados = 'TARJETAS' THEN a.imp_deuda
ELSE COALESCE(w.waguxdex_imp_riesmolo ,0) END
) as deuda_morosa
from bi_corp_staging.garra_wagucdex w
LEFT JOIN bi_corp_common.stk_per_personas pe on pe.cod_per_nup = cast(trim(w.waguxdex_num_persona) as bigint) and pe.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_MORA-Monthly') }}'
LEFT JOIN bi_corp_staging.risksql5_segmentos s ON trim(pe.cod_per_segmentoduro) = trim(s.ramo) and s.fecha_informacion = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_MORA-Monthly') }}'
LEFT JOIN bi_corp_staging.risksql5_productos p ON w.waguxdex_cod_producto = p.codigo_producto and trim(w.waguxdex_cod_subprodu) = trim(p.codigo_subproducto)  and p.fecha_informacion = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_MORA-Monthly') }}'
LEFT JOIN bi_corp_common.stk_mora_flujo_permanencia f ON f.nup = cast(w.waguxdex_num_persona as bigint) and f.num_cuenta = cast(w.waguxdex_num_contrato as bigint) and f.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_MORA-Monthly') }}'
LEFT JOIN deuda_adsf a ON cast(w.waguxdex_num_persona as bigint) = a.cod_per_nup and cast(w.waguxdex_num_contrato as bigint) = a.cod_nro_cuenta and cast(w.waguxdex_cod_centro as bigint) = a.cod_suc_sucursal and cast(w.waguxdex_cod_producto as bigint) = a.cod_prod_producto and w.waguxdex_cod_subprodu = a.cod_prod_subproducto and w.waguxdex_cod_divisa = a.cod_div_divisa
where w.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_MORA-Monthly') }}'
and trim(s.segmento_control) != 'INDIVIDUOS'
and f.inexistente_periodo_actual = 0
group by waguxdex_num_persona
;

CREATE TEMPORARY TABLE primas_genericas as
select periodo,
CAST(cast((cast(pg.porc_cuenta_cte as decimal(17,4)) / 100) as decimal(17,4)) as string) as porc_cuenta_cte,
CAST(cast((cast(pg.porc_personales as decimal(17,4)) / 100) as decimal(17,4)) as string) as porc_personales,
CAST(cast((cast(pg.porc_tarjetas as decimal(17,4)) / 100) as decimal(17,4)) as string) as porc_tarjetas,
CAST(cast((cast(pg.porc_otros as decimal(17,4)) / 100) as decimal(17,4)) as string) as porc_otros,
CAST(cast((cast(pg.porc_hipotecario as decimal(17,4)) / 100) as decimal(17,4)) as string) as porc_hipotecario,
CAST(cast((cast(pg.porc_prendario as decimal(17,4)) / 100) as decimal(17,4)) as string) as porc_prendario,
distribucion
from bi_corp_common.dim_mora_primas_genericas pg
where partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_MORA-Monthly') }}'
;

CREATE TEMPORARY TABLE primas_especificas as
select nup, cod_sucursal, num_cuenta, cod_producto, cod_subproducto, cod_divisa, distribucion_generica,
CAST(cast((cast(pr.redistribuido_cuenta_corriente as decimal(17,4)) / 100) as decimal(17,4)) as string) as redistribuido_cuenta_corriente,
CAST(cast((cast(pr.redistribuido_prestamos_personales as decimal(17,4)) / 100) as decimal(17,4)) as string) as redistribuido_prestamos_personales,
CAST(cast((cast(pr.redistribuido_tarjetas_credito as decimal(17,4)) / 100) as decimal(17,4)) as string) as redistribuido_tarjetas_credito,
CAST(cast((cast(pr.redistribuido_otros as decimal(17,4)) / 100) as decimal(17,4)) as string) as redistribuido_otros,
CAST(cast((cast(pr.redistribuido_hipotecarios as decimal(17,4)) / 100) as decimal(17,4)) as string) as redistribuido_hipotecarios,
CAST(cast((cast(pr.redistribuido_prendarios as decimal(17,4)) / 100) as decimal(17,4)) as string) as redistribuido_prendarios
from bi_corp_common.stk_mora_primas_redistribuidas pr
where partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_MORA-Monthly') }}'
;

CREATE TEMPORARY TABLE pre_ardaman as
SELECT
'{{ ti.xcom_pull(task_ids='InputConfig', key='month', dag_id='LOAD_CMN_MORA-Monthly') }}' as periodo,
w.waguxdex_num_persona as nup,
w.waguxdex_cod_entidad as entidad,
w.waguxdex_cod_centro as centro,
w.waguxdex_num_contrato as contrato,
w.waguxdex_cod_producto as producto,
w.waguxdex_cod_subprodu as subproducto,
w.waguxdex_cod_divisa as divisa,
case when f.inexistente_periodo_actual = 1 and  f.meses_fuera_de_mora_hasta_ingresar between 1 and 12 then 1 else 0 end as marca_cura_pcr09,
case when f.inexistente_periodo_actual = 1 and  f.meses_fuera_de_mora_hasta_ingresar between 1 and 12 then 1 else 0 end as marca_cura_pcr16,
case when f.inexistente_periodo_actual = 0 then 1 else 0 end as mora_pcr09,
case when f.inexistente_periodo_actual = 0 then 1 else 0 end as mora_pcr16,
case when trim(s.segmento_control) = 'INDIVIDUOS' AND (f.inexistente_periodo_actual = 0  OR (w.waguxdex_cod_producto = '70' AND w.waguxdex_cod_subprodu = 'C070' )) then 1
when f.inexistente_periodo_actual = 0 AND (t.deuda_morosa >= (d.deuda_cliente * 5 / 100)) then 1
else 0 end as default_pd_pcr09,
case when trim(s.segmento_control) = 'INDIVIDUOS' AND (f.inexistente_periodo_actual = 0  OR (w.waguxdex_cod_producto = '70' AND w.waguxdex_cod_subprodu = 'C070' )) then 1
when f.inexistente_periodo_actual = 0 AND (t.deuda_morosa >= (d.deuda_cliente * 5 / 100)) then 1
else 0 end as default_pd_pcr16,
case when f.inexistente_periodo_actual = 0 then to_date(f.ingreso_mora) else null end as fecha_entrada_mora404,
case when f.inexistente_periodo_actual = 0 then to_date(f.ingreso_mora) else null end as fecha_entrada_mora404_pcr16,
case when pr.distribucion_generica = 1
then concat_ws('#', pg.porc_tarjetas, pg.porc_personales, pg.porc_cuenta_cte, pg.porc_otros, pg.porc_prendario, pg.porc_hipotecario)
else concat_ws('#', pr.redistribuido_tarjetas_credito, pr.redistribuido_prestamos_personales, pr.redistribuido_cuenta_corriente, pr.redistribuido_otros, pr.redistribuido_prendarios, pr.redistribuido_hipotecarios) end as distribucion_contratos,
case when f.inexistente_periodo_actual = 0 then f.meses_en_mora_hasta_salir else 0 end as meses_en_mora,
case when f.inexistente_periodo_actual = 0 then f.meses_en_mora_lgd else 0 end as meses_en_mora_lgd,
case when f.inexistente_periodo_actual = 0 then f.meses_en_mora_hasta_salir else 0 end as meses_en_mora_pcr16,
case when f.inexistente_periodo_actual = 0 then f.meses_en_mora_lgd else 0 end as meses_en_mora_lgd_pcr16,
r.coef_rating as rating_fecha_alta_contrato,
case when f.inexistente_periodo_actual = 0 then 0 when w.waguxdex_cod_atrib_seg_esp = 'VE' then 1 else 0 end as vigilancia_especial,
0 as subestandar,
null as fecha_entrada_subestandar,
pr.distribucion_generica as distribucion_generica,
w.waguxdex_fec_alt_prod as fecha_alta_contrato,
pe.cod_per_segmentoduro as segmento,
w.waguxdex_imp_riesmolo as imp_riesgo,
w.waguxdex_imp_riesmolo as imp_deuda, -- reemplazar luego con imp deuda de adsf
s.cuadrante as cuadrante,
pe.cod_per_tipopersona as tipo_persona,
s.segmento_control as segmento_control,
case trim(s.segmento_control) when 'INDIVIDUOS' then p.categoria_particulares else p.categoria_carterizados end as descripcion_producto,
t.deuda_morosa as deuda_total_en_mora,
t.deuda_morosa as deuda_total_en_mora_pcr16,
d.deuda_cliente as deuda_total_cliente,
case when d.deuda_cliente = 0 then 0 else (t.deuda_morosa / d.deuda_cliente) END as porcent_para_default_pd_pcr09,
case when d.deuda_cliente = 0 then 0 else (t.deuda_morosa / d.deuda_cliente) END as porcent_para_default_pd_pcr16,
0 as licuado_de_mora_lg,
null as contrato_covid
FROM bi_corp_staging.garra_wagucdex w
LEFT JOIN bi_corp_common.stk_per_personas pe on pe.cod_per_nup = cast(trim(w.waguxdex_num_persona) as bigint) and pe.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_MORA-Monthly') }}'
LEFT JOIN bi_corp_staging.risksql5_segmentos s ON trim(pe.cod_per_segmentoduro) = trim(s.ramo) and s.fecha_informacion = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_MORA-Monthly') }}'
LEFT JOIN bi_corp_staging.risksql5_productos p ON trim(w.waguxdex_cod_producto) = trim(p.codigo_producto) and trim(w.waguxdex_cod_subprodu) = trim(p.codigo_subproducto)  and p.fecha_informacion = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_MORA-Monthly') }}'
LEFT JOIN bi_corp_common.stk_mora_404 m ON m.nup = cast(w.waguxdex_num_persona as bigint) and m.cod_sucursal = cast(w.waguxdex_cod_centro as bigint) and m.num_cuenta = cast(w.waguxdex_num_contrato as bigint) and m.cod_producto = w.waguxdex_cod_producto and m.cod_subproducto = w.waguxdex_cod_subprodu and m.cod_divisa = w.waguxdex_cod_divisa and m.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_MORA-Monthly') }}'
LEFT JOIN primas_especificas pr ON pr.nup = cast(w.waguxdex_num_persona as bigint) and pr.cod_sucursal = cast(w.waguxdex_cod_centro as bigint) and pr.num_cuenta = cast(w.waguxdex_num_contrato as bigint) and pr.cod_producto = w.waguxdex_cod_producto and pr.cod_subproducto = w.waguxdex_cod_subprodu and pr.cod_divisa = w.waguxdex_cod_divisa
LEFT JOIN bi_corp_common.stk_mora_flujo_permanencia f ON f.nup = cast(w.waguxdex_num_persona as bigint) and f.num_cuenta = cast(w.waguxdex_num_contrato as bigint) and f.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_MORA-Monthly') }}'
LEFT JOIN primas_genericas pg ON pg.periodo = '{{ ti.xcom_pull(task_ids='InputConfig', key='month', dag_id='LOAD_CMN_MORA-Monthly') }}' and pg.distribucion = 'r'
LEFT JOIN deuda_cliente d on d.nup = w.waguxdex_num_persona
LEFT JOIN deuda_morosa t on t.nup = w.waguxdex_num_persona
LEFT JOIN rating r on r.num_persona = w.waguxdex_num_persona and r.waguxdex_fec_alt_prod = w.waguxdex_fec_alt_prod
WHERE w.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_MORA-Monthly') }}'
and trim(w.waguxdex_cod_marca) not in ('FA', '')
;

CREATE TEMPORARY TABLE nups_pd_true as
select distinct nup
from pre_ardaman p
where p.default_pd_pcr16  = '1' and trim(p.segmento_control) != 'INDIVIDUOS'
;

CREATE TEMPORARY TABLE ajuste_pd_pcr16 as
select distinct p.nup, p.entidad, p.centro, p.contrato, p.producto, p.subproducto, p.divisa,
CASE WHEN trim(p.segmento_control) != 'INDIVIDUOS' and x.nup is not null THEN 1 ELSE p.default_pd_pcr16 END AS default_pd_pcr16
from pre_ardaman p
LEFT JOIN nups_pd_true x on p.nup = x.nup
;

insert overwrite table bi_corp_common.stk_mora_marcas_riesgos
partition(partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_MORA-Monthly') }}')

SELECT DISTINCT
p.periodo,
p.nup,
p.entidad,
p.centro,
p.contrato,
p.producto,
p.subproducto,
p.divisa,
p.marca_cura_pcr09,
p.marca_cura_pcr16,
CASE WHEN p.mora_pcr16 = 1 and m.num_cuenta is null and p.imp_riesgo != 0 THEN 0 ELSE p.mora_pcr09 END AS mora_pcr09,
CASE WHEN p.mora_pcr16 = 1 and m.num_cuenta is null and p.imp_riesgo != 0 THEN 0 ELSE p.mora_pcr16 END AS mora_pcr16,
case WHEN p.mora_pcr16 = '1' and m.num_cuenta is null and p.imp_riesgo != 0 THEN 0 else x.default_pd_pcr16 END AS default_pd_pcr09,
case WHEN p.mora_pcr16 = '1' and m.num_cuenta is null and p.imp_riesgo != 0 THEN 0 else x.default_pd_pcr16 END AS default_pd_pcr16,
CASE WHEN p.mora_pcr16 = 1 and m.num_cuenta is null and p.imp_riesgo != 0 THEN null ELSE p.fecha_entrada_mora404 END AS fecha_entrada_mora404,
CASE WHEN p.mora_pcr16 = 1 and m.num_cuenta is null and p.imp_riesgo != 0 THEN null ELSE p.fecha_entrada_mora404_pcr16 END AS fecha_entrada_mora404_pcr16,
CASE WHEN t.producto is not null and t.subproducto is not null and trim(p.distribucion_contratos) = ''
THEN concat_ws('#', pg.porc_tarjetas, pg.porc_personales, pg.porc_cuenta_cte, pg.porc_otros, pg.porc_prendario, pg.porc_hipotecario)
ELSE trim(p.distribucion_contratos)
END AS distribucion_contratos,
CASE WHEN p.mora_pcr16 = 1 and m.num_cuenta is null and p.imp_riesgo != 0 THEN 0 ELSE p.meses_en_mora END AS meses_en_mora,
CASE WHEN p.mora_pcr16 = 1 and m.num_cuenta is null and p.imp_riesgo != 0 THEN 0 ELSE p.meses_en_mora_lgd END AS meses_en_mora_lgd,
CASE WHEN p.mora_pcr16 = 1 and m.num_cuenta is null and p.imp_riesgo != 0 THEN 0 ELSE p.meses_en_mora_pcr16 END AS meses_en_mora_pcr16,
CASE WHEN p.mora_pcr16 = 1 and m.num_cuenta is null and p.imp_riesgo != 0 THEN 0 ELSE p.meses_en_mora_lgd_pcr16 END AS meses_en_mora_lgd_pcr16,
p.rating_fecha_alta_contrato,
p.vigilancia_especial,
p.subestandar,
p.fecha_entrada_subestandar,
CASE WHEN t.producto is not null and t.subproducto is not null and trim(p.distribucion_contratos) = '' THEN '1' ELSE trim(p.distribucion_generica) end as distribucion_generica,
p.fecha_alta_contrato,
p.segmento,
p.imp_riesgo,
p.imp_deuda,
p.cuadrante,
p.tipo_persona,
p.segmento_control,
p.descripcion_producto,
p.deuda_total_en_mora,
p.deuda_total_en_mora_pcr16,
p.deuda_total_cliente,
p.porcent_para_default_pd_pcr09,
p.porcent_para_default_pd_pcr16,
p.licuado_de_mora_lg,
p.contrato_covid
FROM pre_ardaman p
LEFT JOIN bi_corp_common.stk_mora_404 m ON cast(p.nup as bigint) = m.nup and p.entidad = m.cod_entidad and cast(p.centro as bigint) = m.cod_sucursal and cast(p.contrato as bigint) = m.num_cuenta and p.producto = m.cod_producto and trim(p.subproducto) = trim(m.cod_subproducto) and p.divisa = m.cod_divisa and m.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_MORA-Monthly') }}'
LEFT JOIN bi_corp_staging.tactico_productos_dist_generica t ON trim(t.producto) = trim(p.producto) and trim(t.subproducto) = trim(p.subproducto)
LEFT JOIN primas_genericas pg ON pg.periodo = '{{ ti.xcom_pull(task_ids='InputConfig', key='month', dag_id='LOAD_CMN_MORA-Monthly') }}' and pg.distribucion = 'r'
LEFT JOIN ajuste_pd_pcr16 x on p.nup = x.nup and p.entidad = x.entidad and p.contrato = x.contrato and p.producto = x.producto and p.subproducto = x.subproducto and p.divisa = x.divisa
;