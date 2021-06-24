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

--traigo maxima fecha de legal entity
CREATE TEMPORARY TABLE IF NOT EXISTS MAX_DATE_LE as
select max(partition_date) max_date
from bi_corp_staging.manual_rosetta_legal_entity_unit_global
where partition_date <= '{last_calendar_day}';

-- inserto nkey credito de riesgo
INSERT OVERWRITE TABLE bi_corp_common.rosetta_nkey
PARTITION (domain_code)
select  distinct
        le.code legal_entity_code,
		ifrs.idf_cto_ifrs native_key,
		case when ifrs.cod_entidad is null
		    then ifrs.idf_cto_ifrs
		        else concat(ifrs.cod_entidad,
                    		ifrs.cod_centro,
                    		ifrs.num_cuenta,
                    		ifrs.cod_producto,
                    		ifrs.cod_subprodu_altair
                    		)
        end master_key,
		'9999-12-31' end_date,
		'{last_calendar_day}' partition_date,
		 gd.domain_code
from santander_business_risk_arda.ifrs9_tabla ifrs,
  	 MAX_DATE_IFRS9 md,
     MAX_TSTAMP_IFRS9 mt,
	 bi_corp_staging.manual_rosetta_global_domains gd,
     bi_corp_staging.manual_rosetta_legal_entity_unit_global le,
     MAX_DATE_GD mgd,
     MAX_DATE_LE mle
where ifrs.data_date_part = md.max_date
and   ifrs.data_timestamp_part = mt.max_timestamp
and   gd.second_name = 'Credit Risk'
and   le.domain = 'LEGAL ENTITY'
and   gd.partition_date = mgd.max_date
and   le.partition_date = mle.max_date;