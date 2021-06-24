SET mapred.job.queue.name=root.dataeng;
SET hive.exec.dynamic.partition.mode=nonstrict;

--traigo maxima fecha de la carga manual de mdim para cr_local
CREATE TEMPORARY TABLE IF NOT EXISTS MAX_DATE_CL_MANUAL as
select max(partition_date) max_date
from bi_corp_staging.manual_cr_local_mdim
where partition_date <= '{last_calendar_day}';

--traigo maxima fecha de la carga manual de mdim para pecs
CREATE TEMPORARY TABLE IF NOT EXISTS MAX_DATE_PECS_MANUAL as
select max(partition_date) max_date
from bi_corp_staging.manual_PECS_mdim
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

-- inserto mdim de cr_local y pecs
INSERT OVERWRITE TABLE bi_corp_common.rosetta_mdim
    PARTITION (domain_code,
               partition_date)
select sg.segmentation_code,
       mcr.attribute_code,
       mcr.name,
       mcr.second_name,
       mcr.hierarchical_level,
       mcr.end_date,
       le.code unit_code,
       gd.domain_code,
       '{last_calendar_day}' partition_date
from bi_corp_staging.manual_cr_local_mdim mcr,
     bi_corp_staging.manual_rosetta_global_domains gd,
     bi_corp_staging.manual_rosetta_segmentation_global sg,
     bi_corp_staging.manual_rosetta_legal_entity_unit_global le,
     MAX_DATE_CL_MANUAL cm,
     MAX_DATE_GD mgd,
     MAX_DATE_GS mgs,
     MAX_DATE_LE mle
where gd.second_name = 'Credit Risk'
and   gd.domain_code = sg.domain_code
and   sg.name = 'Credit Risk Segmentation (Local)'
and   le.domain = 'UNIT GLOBAL'
and   gd.partition_date = mgd.max_date
and   sg.partition_date = mgs.max_date
and   le.partition_date = mle.max_date
and   mcr.partition_date = cm.max_date
union all
select sg.segmentation_code,
       mpec.attribute_code,
       mpec.name,
       mpec.second_name,
       mpec.hierarchical_level,
       mpec.end_date,
       le.code unit_code,
       gd.domain_code,
       '{last_calendar_day}' partition_date
from bi_corp_staging.manual_pecs_mdim mpec,
     bi_corp_staging.manual_rosetta_global_domains gd,
     bi_corp_staging.manual_rosetta_segmentation_global sg,
     bi_corp_staging.manual_rosetta_legal_entity_unit_global le,
     MAX_DATE_PECS_MANUAL mp,
     MAX_DATE_GD mgd,
     MAX_DATE_GS mgs,
     MAX_DATE_LE mle
where gd.second_name = 'Credit Risk'
and   gd.domain_code = sg.domain_code
and   sg.name = 'Comercial and Strategic Plan (PEC)'
and   le.domain = 'UNIT GLOBAL'
and   gd.partition_date = mgd.max_date
and   sg.partition_date = mgs.max_date
and   le.partition_date = mle.max_date
and   mpec.partition_date = mp.max_date;