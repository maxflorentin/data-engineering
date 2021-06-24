set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=4000000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.exec.dynamic.partition.mode=nonstrict;
set mapred.job.queue.name=root.dataeng;

CREATE TEMPORARY TABLE dist_generica_ref as
select
regexp_replace(substr(r.`086_fec_refinanc`, 1, 7), "-", "") as periodo,
sum(r.`086_imp_refnacdo`)  as imp_total_generico,
SUM(CASE WHEN p.categoria_particulares in ('CUENTA CORRIENTE', 'PLAZO FIJO') THEN r.`086_imp_refnacdo` else 0 END) as total_cuenta_corriente,
SUM(CASE p.categoria_particulares WHEN 'PERSONALES' THEN r.`086_imp_refnacdo` else 0 END) as total_personales,
SUM(CASE p.categoria_particulares WHEN 'TARJETAS' THEN r.`086_imp_refnacdo` else 0 END) as total_tarjetas,
SUM(CASE p.categoria_particulares WHEN 'OTROS' THEN r.`086_imp_refnacdo` else 0 END) as total_otros,
SUM(CASE p.categoria_particulares WHEN 'HIPOTECARIO' THEN r.`086_imp_refnacdo` else 0 END) as total_hipotecario,
SUM(CASE p.categoria_particulares WHEN 'PRENDARIO' THEN r.`086_imp_refnacdo` else 0 END) as total_prendario
from bi_corp_staging.garra_contratos_ref r -- on r.`086_num_persona`  = w.waguxdex_num_persona and r.`086_num_contrato` = w.waguxdex_num_contrato and r.partition_date = '2020-11-30' and r.`086_cod_centro` = w.waguxdex_cod_centro  and r.`086_cod_producto` = w.waguxdex_cod_producto and r.`086_cod_subprodu` = w.waguxdex_cod_subprodu
left join bi_corp_staging.risksql5_productos p on trim(p.codigo_producto) = trim(r.`086_cod_productd`) and trim(p.codigo_subproducto) = trim(r.`086_cod_subprodd`) and p.fecha_informacion = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_MORA_PrimasDistribuidas-Daily') }}'
where r.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_MORA_PrimasDistribuidas-Daily') }}'
group by substr(r.`086_fec_refinanc`, 1, 7)
;



CREATE TEMPORARY TABLE propuesta_cru as
select distinct c.nro_propuesta, p.categoria_particulares, regexp_replace(substr(m.mdec1610_fec_ingreso, 1, 7), "-", "") as periodo
from bi_corp_staging.relacion_contrato_cru c
left join bi_corp_staging.moria_contratos_md m on c.cod_centro = m.mdec1610_cod_centro and c.num_contrato = m.mdec1610_num_contrato and c.cod_producto = m.mdec1610_cod_producto and c.cod_subprodu = m.mdec1610_cod_subprodu and c.cod_divisa = m.mdec1610_cod_divisa and m.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_MORA_PrimasDistribuidas-Daily') }}'
left join bi_corp_staging.risksql5_productos p on trim(p.codigo_producto) = trim(c.cod_producto_rel) and trim(p.codigo_subproducto) = trim(c.cod_subprodu_rel) and p.fecha_informacion = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_MORA_PrimasDistribuidas-Daily') }}'
where c.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_MORA_PrimasDistribuidas-Daily') }}'
and c.cod_tipo_rel = 'H'
and porc_deuda != '000,00'
;

CREATE TEMPORARY TABLE dist_generica_cru as
select
periodo,
COUNT(*) as imp_total_generico,
SUM(CASE WHEN categoria_particulares in ('CUENTA CORRIENTE', 'PLAZO FIJO') THEN 1 else 0 END) as total_cuenta_corriente,
SUM(CASE categoria_particulares WHEN 'PERSONALES' THEN 1 else 0 END) as total_personales,
SUM(CASE categoria_particulares WHEN 'TARJETAS' THEN 1 else 0 END) as total_tarjetas,
SUM(CASE categoria_particulares WHEN 'OTROS' THEN 1 else 0 END) as total_otros,
SUM(CASE categoria_particulares WHEN 'HIPOTECARIO' THEN 1 else 0 END) as total_hipotecario,
SUM(CASE categoria_particulares WHEN 'PRENDARIO' THEN 1 else 0 END) as total_prendario
from propuesta_cru
group by periodo
;


INSERT OVERWRITE TABLE bi_corp_common.dim_mora_primas_genericas
PARTITION (partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_MORA_PrimasDistribuidas-Daily') }}')

SELECT x.*
FROM (
select periodo,
round(((total_cuenta_corriente / imp_total_generico) * 100), 2) as porc_cuenta_cte,
round(((total_personales / imp_total_generico) * 100), 2) as porc_personales,
round(((total_tarjetas / imp_total_generico) * 100), 2) as porc_tarjetas,
round(((total_otros / imp_total_generico) * 100), 2) as porc_otros,
round(((total_hipotecario / imp_total_generico) * 100), 2) as porc_hipotecario,
round(((total_prendario / imp_total_generico) * 100), 2) as porc_prendario,
'c' as distribucion
from dist_generica_cru
order by periodo

UNION ALL

select periodo,
round(((total_cuenta_corriente / imp_total_generico) * 100), 2) as porc_cuenta_cte,
round(((total_personales / imp_total_generico) * 100), 2) as porc_personales,
round(((total_tarjetas / imp_total_generico) * 100), 2) as porc_tarjetas,
round(((total_otros / imp_total_generico) * 100), 2) as porc_otros,
round(((total_hipotecario / imp_total_generico) * 100), 2) as porc_hipotecario,
round(((total_prendario / imp_total_generico) * 100), 2) as porc_prendario,
'r' as distribucion
from dist_generica_ref
) x
;
