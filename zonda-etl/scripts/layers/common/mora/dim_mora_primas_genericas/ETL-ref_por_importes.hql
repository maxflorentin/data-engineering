/* -- CALCULO CON IMPORTES
with dist_generica_cru as (
select
substr(m.mdec1610_fec_ingreso, 1, 7) as periodo,
sum(cast(REGEXP_REPLACE(c.imp_saldo_cierre, ",",".") as decimal(17,2)) )  as imp_total_generico,
SUM(CASE WHEN p.categoria_particulares in ('CUENTA CORRIENTE', 'PLAZO FIJO') THEN cast(REGEXP_REPLACE(c.imp_saldo_cierre, ",",".") as decimal(17,2))  else 0 END) as total_cuenta_corriente,
SUM(CASE p.categoria_particulares WHEN 'PERSONALES' THEN cast(REGEXP_REPLACE(c.imp_saldo_cierre, ",",".") as decimal(17,2))  else 0 END) as total_personales,
SUM(CASE p.categoria_particulares WHEN 'TARJETAS' THEN cast(REGEXP_REPLACE(c.imp_saldo_cierre, ",",".") as decimal(17,2))  else 0 END) as total_tarjetas,
SUM(CASE p.categoria_particulares WHEN 'OTROS' THEN cast(REGEXP_REPLACE(c.imp_saldo_cierre, ",",".") as decimal(17,2))  else 0 END) as total_otros,
SUM(CASE p.categoria_particulares WHEN 'HIPOTECARIO' THEN cast(REGEXP_REPLACE(c.imp_saldo_cierre, ",",".") as decimal(17,2))  else 0 END) as total_hipotecario,
SUM(CASE p.categoria_particulares WHEN 'PRENDARIO' THEN cast(REGEXP_REPLACE(c.imp_saldo_cierre, ",",".") as decimal(17,2))  else 0 END) as total_prendario
from bi_corp_staging.relacion_contrato_cru c
left join bi_corp_staging.moria_contratos_md m on c.cod_centro = m.mdec1610_cod_centro and c.num_contrato = m.mdec1610_num_contrato and c.cod_producto = m.mdec1610_cod_producto and c.cod_subprodu = m.mdec1610_cod_subprodu and c.cod_divisa = m.mdec1610_cod_divisa and m.partition_date = '2020-11-30'
left join bi_corp_staging.risksql5_productos p on trim(p.codigo_producto) = trim(c.cod_producto_rel) and trim(p.codigo_subproducto) = trim(c.cod_subprodu_rel) and p.fecha_informacion = '2020-11-30'
where c.partition_date = '2020-11-30' and c.cod_tipo_rel = 'H' --AND substr(m.mdec1610_fec_ingreso, 1, 7) = '2020-10' --AND c.num_persona = '05252434'
group by substr(m.mdec1610_fec_ingreso, 1, 7)
)
select periodo,
round(((total_cuenta_corriente / imp_total_generico) * 100), 2) as porc_cuenta_cte,
round(((total_personales / imp_total_generico) * 100), 2) as porc_personales,
round(((total_tarjetas / imp_total_generico) * 100), 2) as porc_tarjetas,
round(((total_otros / imp_total_generico) * 100), 2) as porc_otros,
round(((total_hipotecario / imp_total_generico) * 100), 2) as porc_hipotecario,
round(((total_prendario / imp_total_generico) * 100), 2) as porc_prendario
from dist_generica_cru
order by periodo
*/