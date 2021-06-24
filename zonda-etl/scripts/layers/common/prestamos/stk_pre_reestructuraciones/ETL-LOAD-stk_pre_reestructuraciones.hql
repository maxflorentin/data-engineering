set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=4000000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.exec.dynamic.partition.mode=nonstrict;
set mapred.job.queue.name=root.dataeng;

DROP TABLE IF EXISTS pedt008;
CREATE TEMPORARY TABLE pedt008 AS
select p.pecodofi, p.penumcon, p.pecodpro, p.pecodsub, p.penumper
from (
      select pecodofi, penumcon, pecodpro, pecodsub, penumper, ROW_NUMBER() OVER (PARTITION BY pecodofi, penumcon, pecodpro, pecodsub ORDER BY coalesce(pefecbrb,'9999-12-31') DESC, peordpar ASC) AS min_firmante
      from bi_corp_staging.malpe_ptb_pedt008
      where partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='max_partition_malpe_ptb_pedt008', dag_id='LOAD_CMN_PRESTAMOS-Daily') }}'
      AND pecalpar = 'TI'
      AND pecodpro IN ('35', '36', '37', '38', '39', '71', '74')
     ) p
where p.min_firmante = 1;

DROP TABLE IF EXISTS pedt70;
CREATE TEMPORARY TABLE pedt70 AS
select pecodofi, penumcon, pecodpro, pecodsub
from bi_corp_staging.malpe_ptb_pedt008
where partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='max_partition_malpe_ptb_pedt008', dag_id='LOAD_CMN_PRESTAMOS-Daily') }}'
AND pecodpro = '70'
AND pecalpar = 'TI'
group by pecodofi, penumcon, pecodpro, pecodsub;

DROP TABLE IF EXISTS normalizaciones;
CREATE TEMPORARY TABLE normalizaciones AS
select norm.*
from bi_corp_staging.normalizacion norm
inner join (select centro, contrato, producto, subproducto, divisa, max(cast(num_sec as int)) as num_sec,partition_date
			from bi_corp_staging.normalizacion
			where partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_PRESTAMOS-Daily') }}'
			group by centro, contrato, producto, subproducto, divisa, partition_date) m on norm.centro=m.centro and norm.contrato=m.contrato and norm.producto=m.producto and norm.subproducto=m.subproducto and norm.divisa=m.divisa and cast(norm.num_sec as int)=m.num_sec and norm.partition_date =m.partition_date
where norm.partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_PRESTAMOS-Daily') }}';

DROP TABLE IF EXISTS normalizados;
CREATE TEMPORARY TABLE normalizados AS
select distinct norm.centro, norm.contrato, norm.producto, norm.subproducto, norm.divisa
from bi_corp_staging.normalizacion norm
where norm.marca_seg_act ='NO'
and norm.fec_normalizacion >= norm.fec_cura
and norm.fec_normalizacion >= norm.fec_cambio_seg
and norm.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_PRESTAMOS-Daily') }}';

DROP TABLE IF EXISTS normalizados_cp;
CREATE TEMPORARY TABLE normalizados_cp AS
select distinct cp.gitccuo_num_persona as num_persona,
cp.gitccuo_cod_centro centro,
cp.gitccuo_num_contrato contrato,
cp.gitccuo_cod_producto producto,
cp.gitccuo_cod_subprodu subproducto,
cp.gitccuo_cod_divisa divisa
from bi_corp_staging.garra_stk_log_cuotaphone cp
where cp.gitccuo_cod_marca_seg_act='NO'
and cp.gitccuo_fec_normaliza >= cp.gitccuo_fec_cura
and cp.gitccuo_fec_normaliza >= cp.gitccuo_fec_cambio_seg
and cp.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_PRESTAMOS-Daily') }}';

DROP TABLE IF EXISTS calculo_reestructuracion_sucesiva;
CREATE TEMPORARY TABLE calculo_reestructuracion_sucesiva AS
select t.`086_num_persona`, t.`086_cod_centro`, t.`086_num_contrato`,max(num_ree) as num_ree
from
(select r1.`086_num_persona`, r1.`086_cod_centro`, r1.`086_num_contrato`,
case when r7.`086_cod_entidad` is not null then 6
     when r6.`086_cod_entidad` is not null then 5
     when r5.`086_cod_entidad` is not null then 4
     when r4.`086_cod_entidad` is not null then 3
     when r3.`086_cod_entidad` is not null then 2
     when r2.`086_cod_entidad` is not null then 1
     else 0 end as num_ree
from bi_corp_staging.garra_contratos_ref r1
left join bi_corp_staging.garra_contratos_ref r2 on r2.`086_num_persona`=r1.`086_num_persona` and r2.`086_cod_centro`=r1.`086_cod_centrod` and r2.`086_num_contrato`=r1.`086_num_contratd` and r2.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_PRESTAMOS-Daily') }}'
left join bi_corp_staging.garra_contratos_ref r3 on r3.`086_num_persona`=r2.`086_num_persona` and r3.`086_cod_centro`=r2.`086_cod_centrod` and r3.`086_num_contrato`=r2.`086_num_contratd` and r3.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_PRESTAMOS-Daily') }}'
left join bi_corp_staging.garra_contratos_ref r4 on r4.`086_num_persona`=r3.`086_num_persona` and r4.`086_cod_centro`=r3.`086_cod_centrod` and r4.`086_num_contrato`=r3.`086_num_contratd` and r4.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_PRESTAMOS-Daily') }}'
left join bi_corp_staging.garra_contratos_ref r5 on r5.`086_num_persona`=r4.`086_num_persona` and r5.`086_cod_centro`=r4.`086_cod_centrod` and r5.`086_num_contrato`=r4.`086_num_contratd` and r5.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_PRESTAMOS-Daily') }}'
left join bi_corp_staging.garra_contratos_ref r6 on r6.`086_num_persona`=r5.`086_num_persona` and r6.`086_cod_centro`=r5.`086_cod_centrod` and r6.`086_num_contrato`=r5.`086_num_contratd` and r6.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_PRESTAMOS-Daily') }}'
left join bi_corp_staging.garra_contratos_ref r7 on r7.`086_num_persona`=r6.`086_num_persona` and r7.`086_cod_centro`=r6.`086_cod_centrod` and r7.`086_num_contrato`=r6.`086_num_contratd` and r7.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_PRESTAMOS-Daily') }}'
where r1.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_PRESTAMOS-Daily') }}'
) t
group by t.`086_num_persona`, t.`086_cod_centro`, t.`086_num_contrato`;

DROP TABLE IF EXISTS tmp_log_cuotaphone;
CREATE TEMPORARY TABLE tmp_log_cuotaphone AS
select s.*
from bi_corp_staging.garra_stk_log_cuotaphone s
inner join (select lc.gitccuo_num_persona,
lc.gitccuo_cod_centro,
lc.gitccuo_num_contrato,
lc.gitccuo_cod_divisa,
lc.partition_date,
max(lc.gitccuo_num_secuencia) gitccuo_num_secuencia
from bi_corp_staging.garra_stk_log_cuotaphone lc
where lc.partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_PRESTAMOS-Daily') }}'
group by lc.gitccuo_num_persona,
lc.gitccuo_cod_centro,
lc.gitccuo_num_contrato,
lc.gitccuo_cod_divisa,
lc.partition_date
) m on
m.gitccuo_num_persona=s.gitccuo_num_persona
and m.gitccuo_cod_centro=s.gitccuo_cod_centro
and m.gitccuo_num_contrato=s.gitccuo_num_contrato
and m.gitccuo_cod_divisa=s.gitccuo_cod_divisa
and m.partition_date=s.partition_date
and m.gitccuo_num_secuencia=s.gitccuo_num_secuencia
where s.partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_PRESTAMOS-Daily') }}';

DROP TABLE IF EXISTS reestructuraciones_mes_cerrado;
CREATE TEMPORARY TABLE reestructuraciones_mes_cerrado AS
select *
from bi_corp_common.stk_pre_reestructuraciones
where partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='previous_month_last_working_day', dag_id='LOAD_CMN_PRESTAMOS-Daily') }}';

DROP TABLE IF EXISTS max_reest_dia_anterior;
CREATE TEMPORARY TABLE max_reest_dia_anterior AS
SELECT max(partition_date) AS partition_date
FROM bi_corp_common.stk_pre_reestructuraciones
WHERE partition_date > date_sub('{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_PRESTAMOS-Daily') }}',7)
and partition_date < '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_PRESTAMOS-Daily') }}';

DROP TABLE IF EXISTS temp_stk_reestructuraciones_dia_anterior;
CREATE TEMPORARY TABLE temp_stk_reestructuraciones_dia_anterior
as
SELECT
    DISTINCT
    cod_suc_sucursal,
    cod_nro_cuenta,
    cod_per_nup,
    cod_prod_producto,
    cod_prod_subproducto,
    cod_div_divisa,
    ds_pre_tiporeestructuracion
FROM bi_corp_common.stk_pre_reestructuraciones s
inner join max_reest_dia_anterior m on m.partition_date=s.partition_date
WHERE s.partition_date > date_sub('{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_PRESTAMOS-Daily') }}',7)
AND s.partition_date < '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_PRESTAMOS-Daily') }}';

DROP TABLE IF EXISTS max_bajas_dia_anterior;
CREATE TEMPORARY TABLE max_bajas_dia_anterior AS
SELECT max(partition_date) AS partition_date
FROM bi_corp_common.bt_pre_bajasreestructuraciones
WHERE partition_date > date_sub('{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_PRESTAMOS-Daily') }}',7)
and partition_date < '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_PRESTAMOS-Daily') }}';

DROP TABLE IF EXISTS temp_stk_bajas_dia_anterior;
CREATE TEMPORARY TABLE temp_stk_bajas_dia_anterior
as
SELECT
    DISTINCT
    cod_suc_sucursal,
    cod_nro_cuenta,
    cod_per_nup,
    cod_prod_producto,
    cod_prod_subproducto,
    cod_div_divisa,
    ds_pre_tiporeestructuracion
FROM bi_corp_common.bt_pre_bajasreestructuraciones s
inner join max_bajas_dia_anterior m on m.partition_date=s.partition_date
WHERE s.partition_date > date_sub('{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_PRESTAMOS-Daily') }}',7)
AND s.partition_date < '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_PRESTAMOS-Daily') }}';


INSERT OVERWRITE TABLE bi_corp_common.stk_pre_reestructuraciones
    PARTITION (partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_PRESTAMOS-Daily') }}')
--Productos 71 y 70 de 71
select
CAST(contratos_di.waguxdex_cod_centro AS INT) as cod_suc_sucursal,
CAST(contratos_di.waguxdex_num_contrato AS BIGINT) as cod_nro_cuenta,
CAST(contratos_di.waguxdex_num_persona AS INT) as cod_per_nup,
contratos_di.waguxdex_cod_producto as cod_prod_producto,
contratos_di.waguxdex_cod_subprodu as cod_prod_subproducto,
contratos_di.waguxdex_cod_divisa as cod_div_divisa,
case when contratos_di.waguxdex_fec_alt_prod in ('0001-01-01','9999-12-31') then null else contratos_di.waguxdex_fec_alt_prod end as dt_pre_fechaaltaproducto,
case when mae.NUM_PROPUES like 'COV%' then coalesce(restAnt.ds_pre_tiporeestructuracion,bajasAnt.ds_pre_tiporeestructuracion) else
(case when coalesce (contratos_di.waguxdex_tip_reestruct,p.tipo_reestructuracion) = '1' then 'RECONDUCCION'
	 when coalesce (contratos_di.waguxdex_tip_reestruct,p.tipo_reestructuracion) = '2' then 'REFINANCIACION'
	 when coalesce (contratos_di.waguxdex_tip_reestruct,p.tipo_reestructuracion) = '3' then 'ACUERDO DE PAGO' end) end AS ds_pre_tiporeestructuracion,
case when mora.num_cuenta is not null then 1 else 0 end as flag_pre_mora,
case when trim(coalesce(contratos_di.waguxdex_cod_atrib_seg_esp,norm.marca_seg_act))='VE' then 1 else 0 end as flag_pre_vigilanciaespecial,
'0' as ds_pre_subestandar,
case when mora.num_cuenta is not null THEN 'MORA' when trim(coalesce(contratos_di.waguxdex_cod_atrib_seg_esp,norm.marca_seg_act))='VE' THEN 'NORMAL_SEGESP' else 'NORMAL' end as ds_pre_estadogestion,
p30.pesegcla as cod_pre_segmento,
trim(s.ds_per_grupo)  as ds_pre_segmento,
s.ds_per_subsegmento as ds_pre_renta,
case trim(s.ds_per_grupo) when 'INDIVIDUOS' then p.categoria_particulares else p.categoria_carterizados end as ds_pre_categoriaproducto,
case when nor.centro is not null then 1 else 0 end as flag_pre_normalizado,
case when coalesce(contratos_di.waguxdex_con_diaimpag,0) = 0 then '0'
     when contratos_di.waguxdex_con_diaimpag >= 1 AND contratos_di.waguxdex_con_diaimpag <= 30 then '1-30'
     when contratos_di.waguxdex_con_diaimpag >= 30 AND contratos_di.waguxdex_con_diaimpag <= 60 then '31-60'
     when contratos_di.waguxdex_con_diaimpag >= 61 AND contratos_di.waguxdex_con_diaimpag <= 90 then '61-90'
     when contratos_di.waguxdex_con_diaimpag >= 91 AND contratos_di.waguxdex_con_diaimpag <= 120 then '91-120'
     when contratos_di.waguxdex_con_diaimpag >= 121 AND contratos_di.waguxdex_con_diaimpag <= 150 then '121-150'
     when contratos_di.waguxdex_con_diaimpag >= 151 AND contratos_di.waguxdex_con_diaimpag <= 180 then '151-180'
     else 'MAS DE 180' end as ds_pre_bucket,
case when trim(coalesce(contratos_di.waguxdex_cod_atrib_seg_esp,norm.marca_seg_act))='' then null else trim(coalesce(contratos_di.waguxdex_cod_atrib_seg_esp,norm.marca_seg_act)) end as cod_pre_tipoclasificacion,
case when trim(contratos_di.waguxdex_motivo_riesgo_sub_est)='' then null else trim(contratos_di.waguxdex_motivo_riesgo_sub_est) end as ds_pre_tipoclasificacion,
case when stk_re.cod_nro_cuenta is not null THEN 'ARRASTRE' ELSE 'INGRESO' END as ds_pre_tipomovimiento,
p.origen_de_la_reestructuracion ds_pre_origendereestructuracion,
coalesce(rs.num_ree,0) as fc_pre_reestructuracionsucesiva,
contratos_di.waguxdex_con_diaimpag as fc_pre_diasatraso,
CAST(contratos_di.waguxdex_imp_riesmolo AS decimal(15,2)) as fc_pre_importeriesgo,
coalesce(cast(mae.impconce as decimal(15,2)),0) as fc_pre_importedispuesto,
CAST(pr.redistribuido_hipotecarios AS decimal(15,2))  as fc_pre_porcentajeprestamohipotecario,
CAST(pr.redistribuido_prendarios AS decimal(15,2)) as fc_pre_porcentajeprestamoprendario,
CAST(pr.redistribuido_prestamos_personales AS decimal(15,2)) as fc_pre_porcentajeprestamopersonales,
CAST(pr.redistribuido_tarjetas_credito AS decimal(15,2)) as fc_pre_porcentajetarjetacredito,
CAST(pr.redistribuido_cuenta_corriente AS decimal(15,2)) as fc_pre_porcentajecuentacorriente,
CAST(pr.redistribuido_otros AS decimal(15,2)) as fc_pre_porcentajeotroproducto,
case when coalesce(cast(mae.impconce as decimal(15,2)),0) = 0 then NULL else (case when mora.num_cuenta is null and coalesce(cast(mae.impconce as decimal(15,2)),0) <> 0 then contratos_di.waguxdex_imp_riesmolo/coalesce(cast(mae.impconce as decimal(15,2)),0) else null end) end as fc_pre_porcentajedeudapendientepago,
case when coalesce(cast(mae.impconce as decimal(15,2)),0) = 0 then NULL else (case when mora.num_cuenta is null and coalesce(cast(mae.impconce as decimal(15,2)),0) <> 0 then 1-contratos_di.waguxdex_imp_riesmolo/coalesce(cast(mae.impconce as decimal(15,2)),0) else null end) end as fc_pre_porcentajedeudapagada,
case when mae.NUM_PROPUES like 'COV%' then 1 else 0 end as flag_pre_marcacovid
from bi_corp_staging.garra_wagucdex contratos_di
left join bi_corp_staging.vmalug_ugdtmae mae on contratos_di.waguxdex_cod_centro=mae.oficina and contratos_di.waguxdex_num_contrato=mae.cuenta and mae.partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_PRESTAMOS-Daily') }}'
left join bi_corp_staging.risksql5_productos p on trim(p.codigo_producto) = contratos_di.waguxdex_cod_producto and trim(p.codigo_subproducto) = contratos_di.waguxdex_cod_subprodu and p.fecha_informacion = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_PRESTAMOS-Daily') }}'
left join normalizaciones norm on norm.centro = contratos_di.waguxdex_cod_centro and norm.contrato = contratos_di.waguxdex_num_contrato and norm.producto = contratos_di.waguxdex_cod_producto and norm.subproducto = contratos_di.waguxdex_cod_subprodu and norm.divisa = contratos_di.waguxdex_cod_divisa
left join normalizados nor on nor.centro = contratos_di.waguxdex_cod_centro and nor.contrato = contratos_di.waguxdex_num_contrato and nor.producto = contratos_di.waguxdex_cod_producto and nor.subproducto = contratos_di.waguxdex_cod_subprodu and nor.divisa = contratos_di.waguxdex_cod_divisa
left join bi_corp_staging.malpe_pedt030 p30 on contratos_di.waguxdex_num_persona = p30.penumper and p30.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_PRESTAMOS-Daily') }}'
left join calculo_reestructuracion_sucesiva rs on contratos_di.waguxdex_num_persona=rs.`086_num_persona` and contratos_di.waguxdex_cod_centro=rs.`086_cod_centro` and contratos_di.waguxdex_num_contrato=rs.`086_num_contrato`
left join bi_corp_common.stk_mora_404 mora on mora.cod_sucursal=cast(contratos_di.waguxdex_cod_centro as bigint) and  mora.num_cuenta=cast(contratos_di.waguxdex_num_contrato as bigint) and mora.cod_producto= contratos_di.waguxdex_cod_producto and mora.cod_subproducto=contratos_di.waguxdex_cod_subprodu and mora.cod_divisa=contratos_di.waguxdex_cod_divisa and mora.partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='max_partition_stk_mora_404', dag_id='LOAD_CMN_PRESTAMOS-Daily') }}'
left join reestructuraciones_mes_cerrado stk_re ON stk_re.cod_suc_sucursal=contratos_di.waguxdex_cod_centro and stk_re.cod_nro_cuenta=contratos_di.waguxdex_num_contrato and stk_re.cod_per_nup=contratos_di.waguxdex_num_persona and stk_re.cod_prod_producto=contratos_di.waguxdex_cod_producto and stk_re.cod_prod_subproducto=contratos_di.waguxdex_cod_subprodu and stk_re.cod_div_divisa=contratos_di.waguxdex_cod_divisa
left join bi_corp_common.dim_per_segmentos s ON trim(p30.pesegcla) = trim(s.cod_per_segmentoduro)
left join bi_corp_common.stk_mora_primas_redistribuidas pr on contratos_di.waguxdex_num_persona=pr.nup and contratos_di.waguxdex_cod_centro=pr.cod_sucursal and contratos_di.waguxdex_num_contrato=pr.num_cuenta and contratos_di.waguxdex_cod_producto=pr.cod_producto and contratos_di.waguxdex_cod_subprodu=pr.cod_subproducto and contratos_di.waguxdex_cod_divisa=pr.cod_divisa and pr.partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='max_partition_stk_mora_primas_redistribuidas', dag_id='LOAD_CMN_PRESTAMOS-Daily') }}'
left join temp_stk_reestructuraciones_dia_anterior restAnt on cast(case when mae.NUM_PROPUES like 'COV%' then SUBSTRING(mae.NUM_PROPUES, 4, 3) else '-1' end as int) = restAnt.cod_suc_sucursal and cast(case when mae.NUM_PROPUES like 'COV%' then SUBSTRING(mae.NUM_PROPUES, 7, 11) else '-1' end as bigint) = restAnt.cod_nro_cuenta
left join temp_stk_bajas_dia_anterior bajasAnt on cast(case when mae.NUM_PROPUES like 'COV%' then SUBSTRING(mae.NUM_PROPUES, 4, 3) else '-1' end as int) = bajasAnt.cod_suc_sucursal and cast(case when mae.NUM_PROPUES like 'COV%' then SUBSTRING(mae.NUM_PROPUES, 7, 11) else '-1' end as bigint) = bajasAnt.cod_nro_cuenta
where contratos_di.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_PRESTAMOS-Daily') }}'
and p.refinanciacion = 'true'
and contratos_di.waguxdex_cod_marca <> 'FA' and contratos_di.waguxdex_cod_marca is not null and TRIM(contratos_di.waguxdex_cod_marca) <> ''

union all
--Productos 39
select
CAST(contratos_di.waguxdex_cod_centro AS INT) as cod_suc_sucursal,
CAST(contratos_di.waguxdex_num_contrato AS BIGINT) as cod_nro_cuenta,
CAST(contratos_di.waguxdex_num_persona AS INT) as cod_per_nup,
contratos_di.waguxdex_cod_producto as cod_prod_producto,
contratos_di.waguxdex_cod_subprodu as cod_prod_subproducto,
contratos_di.waguxdex_cod_divisa as cod_div_divisa,
case when contratos_di.waguxdex_fec_alt_prod in ('0001-01-01','9999-12-31') then null else contratos_di.waguxdex_fec_alt_prod end as dt_pre_fechaaltaproducto,
'RECONDUCCION' AS ds_pre_tiporeestructuracion,
case when mora.num_cuenta is not null then 1 else 0 end as flag_pre_mora,
case when rre_39.clasificacion='VE' then 1 else 0 end as flag_pre_vigilanciaespecial,
'0' as ds_pre_subestandar,
case when mora.num_cuenta is not null THEN 'MORA'
     when rre_39.clasificacion='VE' THEN 'NORMAL_SEGESP'
	 else 'NORMAL' end as ds_pre_estadogestion,
p30.pesegcla as cod_pre_segmento,
trim(s.ds_per_grupo)  as ds_pre_segmento,
s.ds_per_subsegmento as ds_pre_renta,
case trim(s.ds_per_grupo) when 'INDIVIDUOS' then p.categoria_particulares else p.categoria_carterizados end as ds_pre_categoriaproducto,
0 as flag_pre_normalizado,
case when coalesce(contratos_di.waguxdex_con_diaimpag,0) = 0 then '0'
     when contratos_di.waguxdex_con_diaimpag >= 1 AND contratos_di.waguxdex_con_diaimpag <= 30 then '1-30'
     when contratos_di.waguxdex_con_diaimpag >= 30 AND contratos_di.waguxdex_con_diaimpag <= 60 then '31-60'
     when contratos_di.waguxdex_con_diaimpag >= 61 AND contratos_di.waguxdex_con_diaimpag <= 90 then '61-90'
     when contratos_di.waguxdex_con_diaimpag >= 91 AND contratos_di.waguxdex_con_diaimpag <= 120 then '91-120'
     when contratos_di.waguxdex_con_diaimpag >= 121 AND contratos_di.waguxdex_con_diaimpag <= 150 then '121-150'
     when contratos_di.waguxdex_con_diaimpag >= 151 AND contratos_di.waguxdex_con_diaimpag <= 180 then '151-180'
     else 'MAS DE 180' end as ds_pre_bucket,
case when trim(rre_39.clasificacion)='' then null else trim(rre_39.clasificacion) end as cod_pre_tipoclasificacion,
CASE WHEN rre_39.clasificacion='VE' THEN 'CLASIFICACION VIGILANCIA ESPECIAL' WHEN rre_39.clasificacion='DU' THEN 'EN MORIA' end as ds_pre_tipoclasificacion,
case when stk_re.cod_nro_cuenta is not null THEN 'ARRASTRE' ELSE 'INGRESO' END as ds_pre_tipomovimiento,
null as ds_pre_origendereestructuracion,
cast(null as int) as fc_pre_reestructuracionsucesiva,
contratos_di.waguxdex_con_diaimpag as fc_pre_diasatraso,
CAST(contratos_di.waguxdex_imp_riesmolo as decimal(15,2)) as fc_pre_importeriesgo,
CAST(null as decimal(15,2)) as fc_pre_importedispuesto,
CAST(null as decimal(15,2))  as fc_pre_porcentajeprestamohipotecario,
CAST(null as decimal(15,2))  as fc_pre_porcentajeprestamoprendario,
CAST(null as decimal(15,2))  as fc_pre_porcentajeprestamopersonales,
CAST(null as decimal(15,2))  as fc_pre_porcentajetarjetacredito,
CAST(null as decimal(15,2))  as fc_pre_porcentajecuentacorriente,
CAST(null as decimal(15,2))  as fc_pre_porcentajeotroproducto,
CAST(null as decimal(15,2))  as fc_pre_porcentajedeudapendientepago,
CAST(null as decimal(15,2))  as fc_pre_porcentajedeudapagada,
case when mae.NUM_PROPUES like 'COV%' then 1 else 0 end as flag_pre_marcacovid
from bi_corp_staging.garra_wagucdex contratos_di
inner join bi_corp_staging.risksql5_reestructuraciones rre_39 on cast (contratos_di.waguxdex_num_persona as bigint) = cast (rre_39.nup as bigint) and cast (contratos_di.waguxdex_cod_centro as bigint) =cast (rre_39.sucursal as bigint) and cast (contratos_di.waguxdex_num_contrato as bigint) = cast (rre_39.nro_cuenta as bigint) and contratos_di.waguxdex_cod_divisa=rre_39.divisa and rre_39.partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_PRESTAMOS-Daily') }}'
left join bi_corp_staging.risksql5_productos p on trim(p.codigo_producto) = contratos_di.waguxdex_cod_producto and trim(p.codigo_subproducto) = contratos_di.waguxdex_cod_subprodu and p.fecha_informacion = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_PRESTAMOS-Daily') }}'
left join bi_corp_common.stk_mora_404 mora on mora.cod_sucursal=cast(contratos_di.waguxdex_cod_centro as bigint) and  mora.num_cuenta=cast(contratos_di.waguxdex_num_contrato as bigint) and mora.cod_producto= contratos_di.waguxdex_cod_producto and mora.cod_subproducto=contratos_di.waguxdex_cod_subprodu and mora.cod_divisa=contratos_di.waguxdex_cod_divisa and mora.partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='max_partition_stk_mora_404', dag_id='LOAD_CMN_PRESTAMOS-Daily') }}'
left join bi_corp_staging.malpe_pedt030 p30 on contratos_di.waguxdex_num_persona = p30.penumper and p30.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_PRESTAMOS-Daily') }}'
left join reestructuraciones_mes_cerrado stk_re ON stk_re.cod_suc_sucursal=contratos_di.waguxdex_cod_centro and stk_re.cod_nro_cuenta=contratos_di.waguxdex_num_contrato and stk_re.cod_per_nup=contratos_di.waguxdex_num_persona and stk_re.cod_prod_producto=contratos_di.waguxdex_cod_producto and stk_re.cod_prod_subproducto=contratos_di.waguxdex_cod_subprodu and stk_re.cod_div_divisa=contratos_di.waguxdex_cod_divisa
left join bi_corp_common.dim_per_segmentos s ON trim(p30.pesegcla) = trim(s.cod_per_segmentoduro)
left join bi_corp_staging.vmalug_ugdtmae mae on contratos_di.waguxdex_cod_centro=mae.oficina and contratos_di.waguxdex_num_contrato=mae.cuenta and mae.partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_PRESTAMOS-Daily') }}'
left join temp_stk_reestructuraciones_dia_anterior restAnt on cast(case when mae.NUM_PROPUES like 'COV%' then SUBSTRING(mae.NUM_PROPUES, 4, 3) else '-1' end as int) = restAnt.cod_suc_sucursal and cast(case when mae.NUM_PROPUES like 'COV%' then SUBSTRING(mae.NUM_PROPUES, 7, 11) else '-1' end as bigint) = restAnt.cod_nro_cuenta
left join temp_stk_bajas_dia_anterior bajasAnt on cast(case when mae.NUM_PROPUES like 'COV%' then SUBSTRING(mae.NUM_PROPUES, 4, 3) else '-1' end as int) = bajasAnt.cod_suc_sucursal and cast(case when mae.NUM_PROPUES like 'COV%' then SUBSTRING(mae.NUM_PROPUES, 7, 11) else '-1' end as bigint) = bajasAnt.cod_nro_cuenta
where contratos_di.partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_PRESTAMOS-Daily') }}'
and rre_39.partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_PRESTAMOS-Daily') }}'
and contratos_di.waguxdex_cod_marca <> 'FA' and TRIM(contratos_di.waguxdex_cod_marca) <> '' and contratos_di.waguxdex_cod_marca is not null

union all

--PRODUCTOS 40, 41 Y 42
select
CAST(contratos_di.waguxdex_cod_centro AS INT) as cod_suc_sucursal,
CAST(contratos_di.waguxdex_num_contrato AS BIGINT) as cod_nro_cuenta,
CAST(contratos_di.waguxdex_num_persona AS INT) as cod_per_nup,
contratos_di.waguxdex_cod_producto as cod_prod_producto,
contratos_di.waguxdex_cod_subprodu as cod_prod_subproducto,
contratos_di.waguxdex_cod_divisa as cod_div_divisa,
case when contratos_di.waguxdex_fec_alt_prod in ('0001-01-01','9999-12-31') then null else contratos_di.waguxdex_fec_alt_prod end as dt_pre_fechaaltaproducto,
case when contratos_di.waguxdex_tip_reestruct= '1' then 'RECONDUCCION' when contratos_di.waguxdex_tip_reestruct= '2' then 'REFINANCIACION' when contratos_di.waguxdex_tip_reestruct= '3' then 'ACUERDO DE PAGO' end as ds_pre_tiporeestructuracion,
case when mora.num_cuenta is not null then 1 else 0 end as flag_pre_mora,
case when contratos_di.waguxdex_cod_atrib_seg_esp='VE' then 1 else 0 end as flag_pre_vigilanciaespecial,
'0' as ds_pre_subestandar,
case when mora.num_cuenta is not null THEN 'MORA'
     when contratos_di.waguxdex_cod_atrib_seg_esp='VE' THEN 'NORMAL_SEGESP'
	 else 'NORMAL' end as ds_pre_estadogestion,
p30.pesegcla as cod_pre_segmento,
trim(s.ds_per_grupo) as ds_pre_segmento,
s.ds_per_subsegmento as ds_pre_renta,
case trim(s.ds_per_grupo) when 'INDIVIDUOS' then p.categoria_particulares else p.categoria_carterizados end as ds_pre_categoriaproducto,
case when nor.centro is not null then 1 else 0 end as flag_pre_normalizado,
case when coalesce(contratos_di.waguxdex_con_diaimpag,0) = 0 then '0'
     when contratos_di.waguxdex_con_diaimpag >= 1 AND contratos_di.waguxdex_con_diaimpag <= 30 then '1-30'
     when contratos_di.waguxdex_con_diaimpag >= 30 AND contratos_di.waguxdex_con_diaimpag <= 60 then '31-60'
     when contratos_di.waguxdex_con_diaimpag >= 61 AND contratos_di.waguxdex_con_diaimpag <= 90 then '61-90'
     when contratos_di.waguxdex_con_diaimpag >= 91 AND contratos_di.waguxdex_con_diaimpag <= 120 then '91-120'
     when contratos_di.waguxdex_con_diaimpag >= 121 AND contratos_di.waguxdex_con_diaimpag <= 150 then '121-150'
     when contratos_di.waguxdex_con_diaimpag >= 151 AND contratos_di.waguxdex_con_diaimpag <= 180 then '151-180'
     else 'MAS DE 180' end as ds_pre_bucket,
case when trim(contratos_di.waguxdex_cod_atrib_seg_esp)='' then null else trim(contratos_di.waguxdex_cod_atrib_seg_esp) end as cod_pre_tipoclasificacion,
case when trim(contratos_di.waguxdex_motivo_riesgo_sub_est)='' then null else trim(contratos_di.waguxdex_motivo_riesgo_sub_est) end as ds_pre_tipoclasificacion,
case when stk_re.cod_nro_cuenta is not null THEN 'ARRASTRE' ELSE 'INGRESO' END as ds_pre_tipomovimiento,
null as ds_pre_origendereestructuracion,
cast(null as int) as fc_pre_reestructuracionsucesiva,
contratos_di.waguxdex_con_diaimpag as fc_pre_diasatraso,
CAST(contratos_di.waguxdex_imp_riesmolo as decimal(15,2)) as fc_pre_importeriesgo,
CAST(null as decimal(15,2))  as fc_pre_importedispuesto,
CAST(0 as decimal(15,2)) as fc_pre_porcentajeprestamohipotecario,
CAST(0 as decimal(15,2)) as fc_pre_porcentajeprestamoprendario,
CAST(0 as decimal(15,2)) as fc_pre_porcentajeprestamopersonales,
CAST(1 as decimal(15,2)) as fc_pre_porcentajetarjetacredito,
CAST(0 as decimal(15,2))  as fc_pre_porcentajecuentacorriente,
CAST(0 as decimal(15,2))  as fc_pre_porcentajeotroproducto,
CAST(0 as decimal(15,2))  as fc_pre_porcentajedeudapendientepago,
CAST(0 as decimal(15,2))  as fc_pre_porcentajedeudapagada,
0   as flag_pre_marcacovid
from bi_corp_staging.garra_wagucdex contratos_di
left join bi_corp_staging.risksql5_productos p on trim(p.codigo_producto) = contratos_di.waguxdex_cod_producto and trim(p.codigo_subproducto) = contratos_di.waguxdex_cod_subprodu and p.fecha_informacion='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_PRESTAMOS-Daily') }}'
left join bi_corp_common.stk_mora_404 mora on mora.cod_sucursal=cast(contratos_di.waguxdex_cod_centro as bigint) and  mora.num_cuenta=cast(contratos_di.waguxdex_num_contrato as bigint) and mora.cod_producto= contratos_di.waguxdex_cod_producto and mora.cod_subproducto=contratos_di.waguxdex_cod_subprodu and mora.cod_divisa=contratos_di.waguxdex_cod_divisa and mora.partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='max_partition_stk_mora_404', dag_id='LOAD_CMN_PRESTAMOS-Daily') }}'
left join bi_corp_staging.malpe_pedt030 p30 on contratos_di.waguxdex_num_persona = p30.penumper and p30.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_PRESTAMOS-Daily') }}'
left join reestructuraciones_mes_cerrado stk_re ON stk_re.cod_suc_sucursal=contratos_di.waguxdex_cod_centro and stk_re.cod_nro_cuenta=contratos_di.waguxdex_num_contrato and stk_re.cod_per_nup=contratos_di.waguxdex_num_persona and stk_re.cod_prod_producto=contratos_di.waguxdex_cod_producto and stk_re.cod_prod_subproducto=contratos_di.waguxdex_cod_subprodu and stk_re.cod_div_divisa=contratos_di.waguxdex_cod_divisa
left join normalizados_cp nor on nor.centro = contratos_di.waguxdex_cod_centro and nor.contrato = contratos_di.waguxdex_num_contrato and nor.divisa = contratos_di.waguxdex_cod_divisa and nor.num_persona=contratos_di.waguxdex_num_persona
left join bi_corp_common.dim_per_segmentos s ON trim(p30.pesegcla) = trim(s.cod_per_segmentoduro)
where contratos_di.partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_PRESTAMOS-Daily') }}'
and contratos_di.waguxdex_cod_marca <> 'FA' and TRIM(contratos_di.waguxdex_cod_marca) <> '' and contratos_di.waguxdex_cod_marca is not null and categoria_particulares = 'TARJETAS'
and contratos_di.waguxdex_cod_atrib_seg_esp is not null and contratos_di.waguxdex_cod_atrib_seg_esp <> ''

union all
--CUOTAPHONE (que pierden la marca en contratos_di)
select
CAST(contratos_di.waguxdex_cod_centro AS INT) as cod_suc_sucursal,
CAST(contratos_di.waguxdex_num_contrato AS BIGINT) as cod_nro_cuenta,
CAST(contratos_di.waguxdex_num_persona AS INT) as cod_per_nup,
contratos_di.waguxdex_cod_producto as cod_prod_producto,
contratos_di.waguxdex_cod_subprodu as cod_prod_subproducto,
contratos_di.waguxdex_cod_divisa as cod_div_divisa,
case when contratos_di.waguxdex_fec_alt_prod in ('0001-01-01','9999-12-31') then null else contratos_di.waguxdex_fec_alt_prod end as dt_pre_fechaaltaproducto,
case when log_cuo.gitccuo_tip_reestruct_ini= '1' then 'RECONDUCCION' when  log_cuo.gitccuo_tip_reestruct_ini= '2' then 'REFINANCIACION' when log_cuo.gitccuo_tip_reestruct_ini= '3' then 'ACUERDO DE PAGO' end as ds_pre_tiporeestructuracion,
case when mora.num_cuenta is not null then 1 else 0 end as flag_pre_mora,
case when log_cuo.gitccuo_cod_marca_seg_act = 'VE' then 1 else 0 end as flag_pre_vigilanciaespecial,
'0' as ds_pre_subestandar,
case when mora.num_cuenta is not null THEN 'MORA'
     when log_cuo.gitccuo_cod_marca_seg_act='VE' THEN 'NORMAL_SEGESP'
	 else 'NORMAL' end as ds_pre_estadogestion,
p30.pesegcla as cod_pre_segmento,
trim(s.ds_per_grupo)  as ds_pre_segmento,
s.ds_per_subsegmento as ds_pre_renta,
case trim(s.ds_per_grupo) when 'INDIVIDUOS' then p.categoria_particulares else p.categoria_carterizados end as ds_pre_categoriaproducto,
case when nor.centro is not null then 1 else 0 end as flag_pre_normalizado,
case when coalesce(contratos_di.waguxdex_con_diaimpag,0) = 0 then '0'
     when contratos_di.waguxdex_con_diaimpag >= 1 AND contratos_di.waguxdex_con_diaimpag <= 30 then '1-30'
     when contratos_di.waguxdex_con_diaimpag >= 30 AND contratos_di.waguxdex_con_diaimpag <= 60 then '31-60'
     when contratos_di.waguxdex_con_diaimpag >= 61 AND contratos_di.waguxdex_con_diaimpag <= 90 then '61-90'
     when contratos_di.waguxdex_con_diaimpag >= 91 AND contratos_di.waguxdex_con_diaimpag <= 120 then '91-120'
     when contratos_di.waguxdex_con_diaimpag >= 121 AND contratos_di.waguxdex_con_diaimpag <= 150 then '121-150'
     when contratos_di.waguxdex_con_diaimpag >= 151 AND contratos_di.waguxdex_con_diaimpag <= 180 then '151-180'
     else 'MAS DE 180' end as ds_pre_bucket,
case when trim(log_cuo.gitccuo_cod_marca_seg_act)='' then null else trim(log_cuo.gitccuo_cod_marca_seg_act) end as cod_pre_tipoclasificacion,
case when trim(log_cuo.gitccuo_motivo_cambio)='' then null else trim(log_cuo.gitccuo_motivo_cambio) end as ds_pre_tipoclasificacion,
case when stk_re.cod_nro_cuenta is not null THEN 'ARRASTRE' ELSE 'INGRESO' END as ds_pre_tipomovimiento,
null as ds_pre_origendereestructuracion,
cast(null as int) as fc_pre_reestructuracionsucesiva,
contratos_di.waguxdex_con_diaimpag as fc_pre_diasatraso,
CAST(contratos_di.waguxdex_imp_riesmolo as decimal(15,2)) as fc_pre_importeriesgo,
CAST(null as decimal(15,2))  as fc_pre_importedispuesto,
CAST(0 as decimal(15,2)) as fc_pre_porcentajeprestamohipotecario,
CAST(0 as decimal(15,2)) as fc_pre_porcentajeprestamoprendario,
CAST(0 as decimal(15,2)) as fc_pre_porcentajeprestamopersonales,
CAST(1 as decimal(15,2)) as fc_pre_porcentajetarjetacredito,
CAST(0 as decimal(15,2))  as fc_pre_porcentajecuentacorriente,
CAST(0 as decimal(15,2))  as fc_pre_porcentajeotroproducto,
CAST(0 as decimal(15,2))  as fc_pre_porcentajedeudapendientepago,
CAST(0 as decimal(15,2))  as fc_pre_porcentajedeudapagada,
0 as flag_pre_marcacovid
from bi_corp_staging.garra_wagucdex contratos_di
inner join tmp_log_cuotaphone log_cuo on contratos_di.waguxdex_num_persona=log_cuo.gitccuo_num_persona and contratos_di.waguxdex_cod_centro=log_cuo.gitccuo_cod_centro and contratos_di.waguxdex_num_contrato=log_cuo.gitccuo_num_contrato and contratos_di.waguxdex_cod_divisa=log_cuo.gitccuo_cod_divisa and contratos_di.partition_date=log_cuo.partition_date
left join bi_corp_staging.risksql5_productos p on trim(p.codigo_producto) = contratos_di.waguxdex_cod_producto and trim(p.codigo_subproducto) = contratos_di.waguxdex_cod_subprodu and p.fecha_informacion = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_PRESTAMOS-Daily') }}'
left join bi_corp_common.stk_mora_404 mora on mora.cod_sucursal=cast(contratos_di.waguxdex_cod_centro as bigint) and  mora.num_cuenta=cast(contratos_di.waguxdex_num_contrato as bigint) and mora.cod_producto= contratos_di.waguxdex_cod_producto and mora.cod_subproducto=contratos_di.waguxdex_cod_subprodu and mora.cod_divisa=contratos_di.waguxdex_cod_divisa and mora.partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='max_partition_stk_mora_404', dag_id='LOAD_CMN_PRESTAMOS-Daily') }}'
left join bi_corp_staging.malpe_pedt030 p30 on contratos_di.waguxdex_num_persona = p30.penumper and p30.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_PRESTAMOS-Daily') }}'
left join reestructuraciones_mes_cerrado stk_re ON stk_re.cod_suc_sucursal=contratos_di.waguxdex_cod_centro and stk_re.cod_nro_cuenta=contratos_di.waguxdex_num_contrato and stk_re.cod_per_nup=contratos_di.waguxdex_num_persona and stk_re.cod_prod_producto=contratos_di.waguxdex_cod_producto and stk_re.cod_prod_subproducto=contratos_di.waguxdex_cod_subprodu and stk_re.cod_div_divisa=contratos_di.waguxdex_cod_divisa
left join normalizados_cp nor on nor.centro = contratos_di.waguxdex_cod_centro and nor.contrato = contratos_di.waguxdex_num_contrato and nor.divisa = contratos_di.waguxdex_cod_divisa and nor.num_persona=contratos_di.waguxdex_num_persona
left join bi_corp_common.dim_per_segmentos s ON trim(p30.pesegcla) = trim(s.cod_per_segmentoduro)
where contratos_di.partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_PRESTAMOS-Daily') }}'
and contratos_di.waguxdex_cod_marca <> 'FA' and TRIM(contratos_di.waguxdex_cod_marca) <> '' and contratos_di.waguxdex_cod_marca is not null
and (contratos_di.waguxdex_cod_atrib_seg_esp is null OR contratos_di.waguxdex_cod_atrib_seg_esp = '')
and contratos_di.waguxdex_cod_producto='70';