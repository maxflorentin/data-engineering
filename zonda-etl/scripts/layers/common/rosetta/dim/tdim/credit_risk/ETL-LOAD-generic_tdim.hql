SET mapred.job.queue.name=root.dataeng;
SET hive.exec.dynamic.partition.mode=nonstrict;

-- traigo fecha de riesgos/segmentos
CREATE TEMPORARY TABLE IF NOT EXISTS MAX_DATE_RISK_SEG as
select max(fecha_informacion) max_date
from bi_corp_staging.risksql5_segmentos
where fecha_informacion >= trunc('{last_calendar_day}','MM') and fecha_informacion <= '{last_calendar_day}';

-- traigo fecha de riesgos/productos
CREATE TEMPORARY TABLE IF NOT EXISTS MAX_DATE_RISK_PROD as
select max(fecha_informacion) max_date
from bi_corp_staging.risksql5_productos
where fecha_informacion >= trunc('{last_calendar_day}','MM') and fecha_informacion <= '{last_calendar_day}';

--armo tabla de segmentos
CREATE TEMPORARY TABLE IF NOT EXISTS RISK_SEG as
select distinct
case when s.detalle_renta = 'C1'
	 	then 'PYME NEGOCIOS'
	 when s.detalle_renta = 'P1'
	 	then 'PYME 1'
	 when s.detalle_renta = 'P2'
	 	then 'PYME 2'
	 when s.detalle_renta = 'IP'
	 	then 'INSTITUCIONES PRIVADAS'
	 when s.detalle_renta = 'IU'
	 	then 'INSTITUCIONES PUBLICAS'
	 when s.detalle_renta = 'EM'
	 	then 'EMPRESAS'
	 when s.detalle_renta = 'GE'
	 	then 'GRANDES EMPRESAS'
	 when s.detalle_renta = 'ENT FINANC' or (s.detalle_renta = 'COR' and s.desc_marca = 'FIG')
	 	then 'ENTIDADES FINANCIERAS'
	 when s.detalle_renta = 'RENTA SELECT - S'
	 	then 'INDIVIDUOS - RENTA SELECT'
	 when s.detalle_renta = 'RENTA ALTA'
	 	then 'INDIVIDUOS - RENTA ALTA'
	 when s.detalle_renta = 'RENTA MEDIA'
	 	then 'INDIVIDUOS - RENTA MEDIA'
	 when s.detalle_renta = 'RENTA MASIVA'
	 	then 'INDIVIDUOS - RENTA MASIVA'
     when s.detalle_renta = 'COR' and s.desc_marca = 'MRG'
	 	then 'CORPORATE MRG'
	 when s.detalle_renta = 'COR' and s.desc_marca <> 'MRG'
	 	then 'CORPORATE NO MRG'
	 else
	 	s.detalle_renta
	 end segmento
from bi_corp_staging.risksql5_segmentos s,
	 MAX_DATE_RISK_SEG rs
where s.fecha_informacion = rs.max_date;

--armo tabla de productos (se descartan plazos fijos)
CREATE TEMPORARY TABLE IF NOT EXISTS RISK_PROD as
select distinct
case when p.categoria_particulares = 'PRESTAMOS PRENDARIOS'
		then 'PRENDARIO'
	 else
	 	p.categoria_particulares
	 end producto
from bi_corp_staging.risksql5_productos p,
	 MAX_DATE_RISK_PROD rp
where p.fecha_informacion = rp.max_date
and   p.codigo_producto <> '01';

--armo tabla de segmentacion completa
CREATE TEMPORARY TABLE IF NOT EXISTS RISK_TDIM as
select '00001' attribute_code,concat(row_number() over(order by concat(u.segmento,'-',u.producto)),'.0') dimension_value, concat(u.segmento,'-',u.producto) name, null father_attribute_code, null father_dimension_value, '9999-12-31' end_date
from
	(select s.segmento, p.producto
	from RISK_SEG s,
	 	RISK_PROD p
	union all
	select 'CORPORATE MRG' segmento ,'BONOS' producto
	union all
	select 'GOBIERNO' segmento ,'BONOS' producto
	) u;

--traigo maxima fecha de global domains
CREATE TEMPORARY TABLE IF NOT EXISTS MAX_DATE_GD as
select max(partition_date) max_date
from bi_corp_staging.manual_rosetta_global_domains
where partition_date <= '{last_calendar_day}';

--traigo maxima fecha de global segmentation
CREATE TEMPORARY TABLE IF NOT EXISTS MAX_DATE_GS as
select max(partition_date) max_date
from bi_corp_staging.manual_rosetta_segmentation_global
where partition_date <= '{last_calendar_day}';

--traigo maxima fecha de legal entity
CREATE TEMPORARY TABLE IF NOT EXISTS MAX_DATE_LE as
select max(partition_date) max_date
from bi_corp_staging.manual_rosetta_legal_entity_unit_global
where partition_date <= '{last_calendar_day}';

--traigo fecha de mdim cr_local
CREATE TEMPORARY TABLE IF NOT EXISTS MAX_DATE_CR_MDIM as
select max(rm.partition_date) max_date
from bi_corp_common.rosetta_mdim rm,
	 bi_corp_staging.manual_rosetta_global_domains gd,
     bi_corp_staging.manual_rosetta_segmentation_global sg,
     MAX_DATE_GD mgd,
     MAX_DATE_GS mgs
where rm.partition_date <= '{last_calendar_day}'
and   concat(rm.domain_code,rm.segmentation_code) = concat(gd.domain_code,sg.segmentation_code)
and   gd.domain_code = sg.domain_code
and   sg.name = 'Credit Risk Segmentation (Local)'
and   gd.second_name = 'Credit Risk'
and   gd.partition_date = mgd.max_date
and   sg.partition_date = mgs.max_date;

--traigo fecha de mdim pecs
CREATE TEMPORARY TABLE IF NOT EXISTS MAX_DATE_PECS_MDIM as
select max(rm.partition_date) max_date
from bi_corp_common.rosetta_mdim rm,
	 bi_corp_staging.manual_rosetta_global_domains gd,
     bi_corp_staging.manual_rosetta_segmentation_global sg,
     MAX_DATE_GD mgd,
     MAX_DATE_GS mgs
where rm.partition_date <= '{last_calendar_day}'
and   concat(rm.domain_code,rm.segmentation_code) = concat(gd.domain_code,sg.segmentation_code)
and   gd.domain_code = sg.domain_code
and   sg.name = 'Comercial and Strategic Plan (PEC)'
and   gd.second_name = 'Credit Risk'
and   gd.partition_date = mgd.max_date
and   sg.partition_date = mgs.max_date;

--inserto tdim pecs - cr_local
INSERT OVERWRITE TABLE bi_corp_common.rosetta_tdim
    PARTITION (domain_code,
               partition_date)
select
mdim.segmentation_code,
mdim.attribute_code,
rt.dimension_value,
'0' vision,
rt.name,
rt.name second_name,
rt.father_attribute_code,
rt.father_dimension_value,
rt.end_date,
le.code unit_code,
mdim.domain_code,
'{last_calendar_day}' partition_date
from bi_corp_common.rosetta_mdim mdim,
     bi_corp_staging.manual_rosetta_segmentation_global sg,
     bi_corp_staging.manual_rosetta_legal_entity_unit_global le,
     RISK_TDIM rt,
     MAX_DATE_PECS_MDIM mdp,
     MAX_DATE_GS mgs,
     MAX_DATE_LE mle
where mdim.domain_code = sg.domain_code
and   mdim.segmentation_code = sg.segmentation_code
and   sg.name = 'Comercial and Strategic Plan (PEC)'
and   le.domain = 'UNIT GLOBAL'
and   rt.attribute_code = mdim.attribute_code
and   mdim.partition_date = mdp.max_date
and   sg.partition_date = mgs.max_date
and   le.partition_date = mle.max_date
union all
select
mdim.segmentation_code,
mdim.attribute_code,
rt.dimension_value,
'0' vision,
rt.name,
rt.name second_name,
rt.father_attribute_code,
rt.father_dimension_value,
rt.end_date,
le.code unit_code,
mdim.domain_code,
'{last_calendar_day}' partition_date
from bi_corp_common.rosetta_mdim mdim,
     bi_corp_staging.manual_rosetta_segmentation_global sg,
     bi_corp_staging.manual_rosetta_legal_entity_unit_global le,
     RISK_TDIM rt,
     MAX_DATE_CR_MDIM mcr,
     MAX_DATE_GS mgs,
     MAX_DATE_LE mle
where mdim.domain_code = sg.domain_code
and   mdim.segmentation_code = sg.segmentation_code
and   sg.name = 'Credit Risk Segmentation (Local)'
and   le.domain = 'UNIT GLOBAL'
and   rt.attribute_code = mdim.attribute_code
and   mdim.partition_date = mcr.max_date
and   sg.partition_date = mgs.max_date
and   le.partition_date = mle.max_date;