set hive.exec.dynamic.partition.mode=nonstrict;
set mapred.job.queue.name=root.dataeng;

--traigo maxima fecha de la tabla IRSF9
CREATE TEMPORARY TABLE IF NOT EXISTS MAX_DATE_IFRS9 as
select max(ifr.data_date_part) max_date
from santander_business_risk_arda.ifrs9_tabla ifr
where to_date(from_unixtime(UNIX_TIMESTAMP(ifr.data_date_part ,'yyyyMMdd'))) >= trunc('{last_calendar_day}','MM')
and   to_date(from_unixtime(UNIX_TIMESTAMP(ifr.data_date_part ,'yyyyMMdd'))) <= '{last_calendar_day}';

--traigo maxima time stamp del mes de analisis de la tabla IRSF9
CREATE TEMPORARY TABLE IF NOT EXISTS MAX_TSTAMP_IFRS9 as
select max(ifr.data_timestamp_part) max_timestamp
from santander_business_risk_arda.ifrs9_tabla ifr,
     MAX_DATE_IFRS9 md
where ifr.data_date_part = md.max_date;

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

-- armo tabla de segmentos para cruzar contra tdim
CREATE TEMPORARY TABLE IF NOT EXISTS PARAM_SEG as
select
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
	 end parametro,
	 trim(s.ramo) dimension
from bi_corp_staging.risksql5_segmentos s,
	 MAX_DATE_RISK_SEG rs
where s.fecha_informacion = rs.max_date;

-- armo tabla de producto/subproducto para cruzar contra tdim
CREATE TEMPORARY TABLE IF NOT EXISTS PARAM_PROD as
select
case when p.categoria_particulares = 'PRESTAMOS PRENDARIOS'
		then 'PRENDARIO'
	 else
	 	p.categoria_particulares
	 end parametro,
	 concat (p.codigo_producto,trim(p.codigo_subproducto)) dimension
from bi_corp_staging.risksql5_productos p,
	 MAX_DATE_RISK_PROD rp
where p.fecha_informacion = rp.max_date
and   p.codigo_producto <> '01';

--armo tabla de segmentacion completa
CREATE TEMPORARY TABLE IF NOT EXISTS PARAM_RISK as
select concat(s.parametro,'-',p.parametro) parametro,
	   concat(s.dimension,p.dimension) dimension
from PARAM_SEG s,
	 PARAM_PROD p;

--armo segmentacion completa
CREATE TEMPORARY TABLE IF NOT EXISTS SEGMENTATION_CR as
select ifrs.idf_cto_ifrs,
	   pr.parametro
from santander_business_risk_arda.ifrs9_tabla ifrs
	 inner join MAX_DATE_IFRS9 md
    	on (ifrs.data_date_part = md.max_date)
     inner join MAX_TSTAMP_IFRS9 mt
    	on(ifrs.data_timestamp_part = mt.max_timestamp)
     left join PARAM_RISK pr
    	on (concat(ifrs.segmento_duro,
           ifrs.cod_producto,
        	ifrs.cod_subprodu_altair) = pr.dimension
           )
where ifrs.cod_producto is not null
union all
select distinct
	  ifrs.idf_cto_ifrs,
	   concat(case when esp.cod_portafolio = 'ARG-CORPORATE'
			   	then 'CORPORATE MRG'
			  else
					 'GOBIERNO'
	   		  end,
	   		  '-BONOS'
	   		 ) parametro
from
    santander_business_risk_arda.ifrs9_tabla ifrs
    inner join MAX_DATE_IFRS9 md
    	on (ifrs.data_date_part = md.max_date)
    inner join MAX_TSTAMP_IFRS9 mt
    	on (ifrs.data_timestamp_part = mt.max_timestamp)
    left join santander_business_risk_arda.especies esp
    	on (ifrs.idf_cto_ifrs = esp.cod_especie
    	   and ifrs.data_date_part = esp.data_date_part)
where ifrs.cod_producto is null;

--traigo fecha de tdim cr_local
CREATE TEMPORARY TABLE IF NOT EXISTS MAX_DATE_CR_TDIM as
select max(rt.partition_date) max_date
from bi_corp_common.rosetta_tdim rt,
	 bi_corp_staging.manual_rosetta_global_domains gd,
     bi_corp_staging.manual_rosetta_segmentation_global sg,
     MAX_DATE_GD mgd,
     MAX_DATE_GS mgs
where rt.partition_date <= '{last_calendar_day}'
and   concat(rt.domain_code,rt.segmentation_code) = concat(gd.domain_code,sg.segmentation_code)
and   gd.domain_code = sg.domain_code
and   sg.name = 'Credit Risk Segmentation (Local)'
and   gd.second_name = 'Credit Risk'
and   gd.partition_date = mgd.max_date
and   sg.partition_date = mgs.max_date;

--traigo fecha de tdim pecs
CREATE TEMPORARY TABLE IF NOT EXISTS MAX_DATE_PECS_TDIM as
select max(rt.partition_date) max_date
from bi_corp_common.rosetta_tdim rt,
	 bi_corp_staging.manual_rosetta_global_domains gd,
     bi_corp_staging.manual_rosetta_segmentation_global sg,
     MAX_DATE_GD mgd,
     MAX_DATE_GS mgs
where rt.partition_date <= '{last_calendar_day}'
and   concat(rt.domain_code,rt.segmentation_code) = concat(gd.domain_code,sg.segmentation_code)
and   gd.domain_code = sg.domain_code
and   sg.name = 'Comercial and Strategic Plan (PEC)'
and   gd.second_name = 'Credit Risk'
and   gd.partition_date = mgd.max_date
and   sg.partition_date = mgs.max_date;

--armo tdim con la fecha de analisis solo para CR
CREATE TEMPORARY TABLE IF NOT EXISTS CR_TDIM as
select rt.segmentation_code,
	   rt.attribute_code,
	   rt.dimension_value,
	   rt.name
from bi_corp_common.rosetta_tdim rt,
	 bi_corp_staging.manual_rosetta_global_domains gd,
     bi_corp_staging.manual_rosetta_segmentation_global sg,
     MAX_DATE_GD mgd,
     MAX_DATE_GS mgs,
     MAX_DATE_CR_TDIM mt
where rt.partition_date <= '{last_calendar_day}'
and   concat(rt.domain_code,rt.segmentation_code) = concat(gd.domain_code,sg.segmentation_code)
and   gd.domain_code = sg.domain_code
and   sg.name = 'Credit Risk Segmentation (Local)'
and   gd.second_name = 'Credit Risk'
and   gd.partition_date = mgd.max_date
and   sg.partition_date = mgs.max_date
and   rt.partition_date = mt.max_date;

--armo tdim con la fecha de analisis solo para Pecs
CREATE TEMPORARY TABLE IF NOT EXISTS PECS_TDIM as
select rt.segmentation_code,
	   rt.attribute_code,
	   rt.dimension_value,
	   rt.name
from bi_corp_common.rosetta_tdim rt,
	 bi_corp_staging.manual_rosetta_global_domains gd,
     bi_corp_staging.manual_rosetta_segmentation_global sg,
     MAX_DATE_GD mgd,
     MAX_DATE_GS mgs,
     MAX_DATE_PECS_TDIM pt
where rt.partition_date <= '{last_calendar_day}'
and   concat(rt.domain_code,rt.segmentation_code) = concat(gd.domain_code,sg.segmentation_code)
and   gd.domain_code = sg.domain_code
and   sg.name = 'Comercial and Strategic Plan (PEC)'
and   gd.second_name = 'Credit Risk'
and   gd.partition_date = mgd.max_date
and   sg.partition_date = mgs.max_date
and   rt.partition_date = pt.max_date;

-- inserto credit risk
INSERT OVERWRITE TABLE bi_corp_common.rosetta_rnkd
PARTITION (domain_code)
select distinct
le.code,
cr.idf_cto_ifrs,
sg.segmentation_code,
tdim.attribute_code,
null local_value,
tdim.dimension_value global_value,
sg.segmentation_type,
'9999-12-31' end_date,
'{last_calendar_day}' partition_date,
sg.domain_code
from
    SEGMENTATION_CR cr
    inner join bi_corp_staging.manual_rosetta_legal_entity_unit_global le
        on (le.domain = 'LEGAL ENTITY')
    inner join MAX_DATE_LE mle
	 	on (le.partition_date = mle.max_date)
    inner join bi_corp_staging.manual_rosetta_segmentation_global sg
        on (sg.name = 'Credit Risk Segmentation (Local)')
    inner join MAX_DATE_GS mgs
        on (sg.partition_date = mgs.max_date)
    left join CR_TDIM tdim
        on (cr.parametro = tdim.name)
union all
select distinct
le.code,
cr.idf_cto_ifrs,
sg.segmentation_code,
tdim.attribute_code,
null local_value,
tdim.dimension_value global_value,
sg.segmentation_type,
'9999-12-31' end_date,
'{last_calendar_day}' partition_date,
sg.domain_code
from
    SEGMENTATION_CR cr
    inner join bi_corp_staging.manual_rosetta_legal_entity_unit_global le
        on (le.domain = 'LEGAL ENTITY')
    inner join MAX_DATE_LE mle
	 	on (le.partition_date = mle.max_date)
    inner join bi_corp_staging.manual_rosetta_segmentation_global sg
        on (sg.name = 'Comercial and Strategic Plan (PEC)')
    inner join MAX_DATE_GS mgs
        on (sg.partition_date = mgs.max_date)
    left join PECS_TDIM tdim
        on (cr.parametro = tdim.name);