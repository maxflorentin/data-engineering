SET mapred.job.queue.name=root.dataeng;
SET hive.exec.dynamic.partition.mode=nonstrict;

--traigo maxima fecha de la carga manual de tdim para icaap
CREATE TEMPORARY TABLE IF NOT EXISTS MAX_DATE_ICAAP_MANUAL as
select max(partition_date) max_date
from bi_corp_staging.manual_icaap_tdim
where partition_date <= '{last_calendar_day}';

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

--traigo fecha de mdim icaap
CREATE TEMPORARY TABLE IF NOT EXISTS MAX_DATE_ICAAP_MDIM as
select max(rm.partition_date) max_date
from bi_corp_common.rosetta_mdim rm,
	 bi_corp_staging.manual_rosetta_global_domains gd,
     bi_corp_staging.manual_rosetta_segmentation_global sg,
     MAX_DATE_GD mgd,
     MAX_DATE_GS mgs
where rm.partition_date <=  '{last_calendar_day}'
and   concat(rm.domain_code,rm.segmentation_code) = concat(gd.domain_code,sg.segmentation_code)
and   gd.domain_code = sg.domain_code
and   sg.name = 'ICAAP Segmentation (Local)'
and   gd.second_name = 'ICAAP'
and   gd.partition_date = mgd.max_date
and   sg.partition_date = mgs.max_date;

--inserto tdim icaap
INSERT OVERWRITE TABLE bi_corp_common.rosetta_tdim
    PARTITION (domain_code,
               partition_date)
select
mdim.segmentation_code,
mdim.attribute_code,
mit.dimension_value,
'0' vision,
mit.name,
mit.second_name,
mit.father_attribute_code,
mit.father_dimension_value,
mit.end_date,
le.code unit_code,
mdim.domain_code,
'{last_calendar_day}' partition_date
from bi_corp_common.rosetta_mdim mdim,
     bi_corp_staging.manual_rosetta_segmentation_global sg,
     bi_corp_staging.manual_rosetta_legal_entity_unit_global le,
	 bi_corp_staging.manual_icaap_tdim mit,
	 MAX_DATE_ICAAP_MANUAL icm,
     MAX_DATE_ICAAP_MDIM mdi,
     MAX_DATE_GS mgs,
     MAX_DATE_LE mle
where mdim.domain_code = sg.domain_code
and   mdim.segmentation_code = sg.segmentation_code
and   sg.name = 'ICAAP Segmentation (Local)'
and   le.domain = 'UNIT GLOBAL'
and   mit.partition_date = icm.max_date
and   mit.attribute_code = mdim.attribute_code
and   mdim.partition_date = mdi.max_date
and   sg.partition_date = mgs.max_date
and   le.partition_date = mle.max_date;