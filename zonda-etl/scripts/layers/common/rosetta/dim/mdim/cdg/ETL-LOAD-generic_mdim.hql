SET mapred.job.queue.name=root.dataeng;
SET hive.exec.dynamic.partition.mode=nonstrict;

--traigo maxima fecha de la carga manual de mdim para cdg
CREATE TEMPORARY TABLE IF NOT EXISTS MAX_DATE_CDG_MANUAL as
select max(partition_date) max_date
from bi_corp_staging.manual_cdg_mdim
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

-- inserto en mdim manual cdg
INSERT OVERWRITE TABLE bi_corp_common.rosetta_mdim
    PARTITION (domain_code,
               partition_date)
select sg.segmentation_code,
       mcg.attribute_code,
       mcg.name,
       mcg.second_name,
       mcg.hierarchical_level,
       mcg.end_date,
       le.code unit_code,
       gd.domain_code,
       '{last_calendar_day}' partition_date
from bi_corp_staging.manual_cdg_mdim mcg,
     bi_corp_staging.manual_rosetta_global_domains gd,
     bi_corp_staging.manual_rosetta_segmentation_global sg,
     bi_corp_staging.manual_rosetta_legal_entity_unit_global le,
     MAX_DATE_CDG_MANUAL cm,
     MAX_DATE_GD mgd,
     MAX_DATE_GS mgs,
     MAX_DATE_LE mle
where gd.second_name = 'Control de Gestion'
and   gd.domain_code = sg.domain_code
and   sg.name = 'Local Business Area Segmentation'
and   le.domain = 'UNIT GLOBAL'
and   gd.partition_date = mgd.max_date
and   sg.partition_date = mgs.max_date
and   le.partition_date = mle.max_date
and   mcg.partition_date = cm.max_date;