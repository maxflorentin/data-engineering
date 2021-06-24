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

-- traigo fecha de segmentos
CREATE TEMPORARY TABLE IF NOT EXISTS MAX_DATE_RISK_SEG as
select max(fecha_informacion) max_date
from bi_corp_staging.risksql5_segmentos
where fecha_informacion >= trunc('{last_calendar_day}','MM') and fecha_informacion <= '{last_calendar_day}';

-- armo tabla de segmentos
CREATE TEMPORARY TABLE IF NOT EXISTS PARAM_SEG as
select case when SUBSTRING(trim(s.ramo),1,1) in ('0','3','9','J','K','L','M','N','O','P','Q','R','S','T','U','W','X')
	   	then '5.0'
	   	 	when SUBSTRING(trim(s.ramo),1,1) in ('1','2','Y','Z')
	   	then '3.0'
	   		when SUBSTRING(trim(s.ramo),1,1) in ('4','5','6','7','8','A','B','C','D','E','F','G','H','I')
		then '6.0'
	   		when SUBSTRING(trim(s.ramo),1,1) in ('V')
	   	then '2.0'
	   end parametro,
	 trim(s.ramo) dimension
from bi_corp_staging.risksql5_segmentos s,
	 MAX_DATE_RISK_SEG rs
where s.fecha_informacion = rs.max_date;

--armo segmentacion completa
CREATE TEMPORARY TABLE IF NOT EXISTS SEGMENTATION_FR as
select ifrs.idf_cto_ifrs,
	   ps.parametro
from santander_business_risk_arda.ifrs9_tabla ifrs
	 inner join MAX_DATE_IFRS9 md
    	on (ifrs.data_date_part = md.max_date)
     inner join MAX_TSTAMP_IFRS9 mt
    	on(ifrs.data_timestamp_part = mt.max_timestamp)
     left join PARAM_SEG ps
    	on (ifrs.segmento_duro = ps.dimension)
where ifrs.cod_producto is not null
union all
select distinct
	  ifrs.idf_cto_ifrs,
	  case when esp.cod_emisor = 'BCRA'
	  	then '1.0'
	  else
			 '2.0'
	  end parametro
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

-- inserto finrep
INSERT OVERWRITE TABLE bi_corp_common.rosetta_rnkd
PARTITION (domain_code)
select distinct
le.code,
fr.idf_cto_ifrs,
sg.segmentation_code,
'00001' attribute_code,
null local_value,
fr.parametro global_value,
sg.segmentation_type,
'9999-12-31' end_date,
'{last_calendar_day}' partition_date,
sg.domain_code
from
    SEGMENTATION_FR fr
    inner join bi_corp_staging.manual_rosetta_legal_entity_unit_global le
        on (le.domain = 'LEGAL ENTITY')
    inner join MAX_DATE_LE mle
	 	on (le.partition_date = mle.max_date)
    inner join bi_corp_staging.manual_rosetta_segmentation_global sg
        on (sg.name = 'FINREP Segmentation')
    inner join MAX_DATE_GS mgs
        on (sg.partition_date = mgs.max_date);