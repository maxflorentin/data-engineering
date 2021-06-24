SET mapred.job.queue.name=root.dataeng;
SET hive.exec.dynamic.partition.mode=nonstrict;

--traigo maxima fecha de la carga manual de mdim para icaap
CREATE TEMPORARY TABLE IF NOT EXISTS MAX_DATE_ICAAP_MANUAL as
select max(partition_date) max_date
from bi_corp_staging.manual_icaap_mdim
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

-- inserto en mdim manual icaap
INSERT OVERWRITE TABLE bi_corp_common.rosetta_mdim
    PARTITION (domain_code,
               partition_date)
select sg.segmentation_code,
       mip.attribute_code,
       mip.name,
       mip.second_name,
       mip.hierarchical_level,
       mip.end_date,
       le.code unit_code,
       gd.domain_code,
       '{last_calendar_day}' partition_date
from bi_corp_staging.manual_icaap_mdim mip,
     bi_corp_staging.manual_rosetta_global_domains gd,
     bi_corp_staging.manual_rosetta_segmentation_global sg,
     bi_corp_staging.manual_rosetta_legal_entity_unit_global le,
     MAX_DATE_ICAAP_MANUAL im,
     MAX_DATE_GD mgd,
     MAX_DATE_GS mgs,
     MAX_DATE_LE mle
where gd.second_name = 'ICAAP'
and   gd.domain_code = sg.domain_code
and   sg.name = 'ICAAP Segmentation (Local)'
and   le.domain = 'UNIT GLOBAL'
and   gd.partition_date = mgd.max_date
and   sg.partition_date = mgs.max_date
and   le.partition_date = mle.max_date
and   mip.partition_date = im.max_date;