SET mapred.job.queue.name=root.dataeng;
SET hive.exec.dynamic.partition.mode=nonstrict;

-- traigo fecha de adn de cdg
CREATE TEMPORARY TABLE IF NOT EXISTS MAX_DATE_CGD_SEG_ADN as
select max(partition_date) max_date
from bi_corp_staging.rio157_ms0_dm_dwh_area_negocio_ctr
where partition_date >= trunc('{last_calendar_day}','MM')
and partition_date <= '{last_calendar_day}';

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

--armo tabla de dimensiones pivoteada
CREATE TEMPORARY TABLE IF NOT EXISTS TEMP_ROSETTA_CDG_SEGMENTATION as
select distinct * from (
select
      '00001' attribute_code,cod_area_negocio_niv_0 dimension_value, des_area_negocio_niv_0 name, null father_attribute_code, null father_dimension_value, '9999-12-31' end_date
from bi_corp_staging.rio157_ms0_dm_dwh_area_negocio_ctr,
     MAX_DATE_CGD_SEG_ADN md
where partition_date = md.max_date
and cod_jerq_adn01 = 'JAN01'
and cod_area_negocio_niv_0 <> 'null'
union all
select
      '00002' attribute_code,cod_area_negocio_niv_1 dimension_value, des_area_negocio_niv_1 name, '00001' father_attribute_code, cod_area_negocio_niv_0 father_dimension_value, '9999-12-31' end_date
from bi_corp_staging.rio157_ms0_dm_dwh_area_negocio_ctr,
     MAX_DATE_CGD_SEG_ADN md
where partition_date = md.max_date
and cod_jerq_adn01 = 'JAN01'
and cod_area_negocio_niv_1 <> 'null'
and cod_area_negocio_niv_1 <> cod_area_negocio_niv_0
union all
select
      '00003' attribute_code, cod_area_negocio_niv_2 dimension_value,des_area_negocio_niv_2 name,'00002' father_attribute_code,cod_area_negocio_niv_1 father_dimension_value,'9999-12-31' end_date
from bi_corp_staging.rio157_ms0_dm_dwh_area_negocio_ctr,
     MAX_DATE_CGD_SEG_ADN md
where partition_date = md.max_date
and cod_jerq_adn01 = 'JAN01'
and cod_area_negocio_niv_2 <> 'null'
and cod_area_negocio_niv_2 <> cod_area_negocio_niv_1
union all
select
      '00004' attribute_code, cod_area_negocio_niv_3 dimension_value,des_area_negocio_niv_3 name,'00003' father_attribute_code,cod_area_negocio_niv_2 father_dimension_value,'9999-12-31' end_date
from bi_corp_staging.rio157_ms0_dm_dwh_area_negocio_ctr,
     MAX_DATE_CGD_SEG_ADN md
where partition_date = md.max_date
and cod_jerq_adn01 = 'JAN01'
and cod_area_negocio_niv_3 <> 'null'
and cod_area_negocio_niv_3 <> cod_area_negocio_niv_2
union all
select
      '00005' attribute_code, cod_area_negocio_niv_4 dimension_value,des_area_negocio_niv_4 name,'00004' father_attribute_code,cod_area_negocio_niv_3 father_dimension_value,'9999-12-31' end_date
from bi_corp_staging.rio157_ms0_dm_dwh_area_negocio_ctr,
     MAX_DATE_CGD_SEG_ADN md
where partition_date = md.max_date
and cod_jerq_adn01 = 'JAN01'
and cod_area_negocio_niv_4 <> 'null'
and cod_area_negocio_niv_4 <> cod_area_negocio_niv_3
union all
select
      '00006' attribute_code, cod_area_negocio_niv_5 dimension_value,des_area_negocio_niv_5 name,'00005' father_attribute_code,cod_area_negocio_niv_4 father_dimension_value,'9999-12-31' end_date
from bi_corp_staging.rio157_ms0_dm_dwh_area_negocio_ctr,
     MAX_DATE_CGD_SEG_ADN md
where partition_date = md.max_date
and cod_jerq_adn01 = 'JAN01'
and cod_area_negocio_niv_5 <> 'null'
and cod_area_negocio_niv_5 <> cod_area_negocio_niv_4
union all
select
      '00007' attribute_code, cod_area_negocio_niv_6 dimension_value ,des_area_negocio_niv_6 name,'00006' father_attribute_code,cod_area_negocio_niv_5 father_dimension_value,'9999-12-31' end_date
from bi_corp_staging.rio157_ms0_dm_dwh_area_negocio_ctr,
     MAX_DATE_CGD_SEG_ADN md
where partition_date = md.max_date
and cod_jerq_adn01 = 'JAN01'
and cod_area_negocio_niv_6 <> 'null'
and cod_area_negocio_niv_6 <> cod_area_negocio_niv_5
union all
select
      '00008' attribute_code, cod_area_negocio_niv_7 dimension_value,des_area_negocio_niv_7 name,'00007' father_attribute_code,cod_area_negocio_niv_6 father_dimension_value,'9999-12-31' end_date
from bi_corp_staging.rio157_ms0_dm_dwh_area_negocio_ctr,
     MAX_DATE_CGD_SEG_ADN md
where partition_date = md.max_date
and cod_jerq_adn01 = 'JAN01'
and cod_area_negocio_niv_7 <> 'null'
and cod_area_negocio_niv_7 <> cod_area_negocio_niv_6
union all
select
      '00009' attribute_code, cod_area_negocio_niv_8 dimension_value,des_area_negocio_niv_8 name,'00008' father_attribute_code,cod_area_negocio_niv_7 father_dimension_value,'9999-12-31' end_date
from bi_corp_staging.rio157_ms0_dm_dwh_area_negocio_ctr,
     MAX_DATE_CGD_SEG_ADN md
where partition_date = md.max_date
and cod_jerq_adn01 = 'JAN01'
and cod_area_negocio_niv_8 <> 'null'
and cod_area_negocio_niv_8 <> cod_area_negocio_niv_7
union all
select
      '00010' attribute_code, cod_area_negocio_niv_9 dimension_value,des_area_negocio_niv_9 name,'00009' father_attribute_code,cod_area_negocio_niv_8 father_dimension_value,'9999-12-31' end_date
from bi_corp_staging.rio157_ms0_dm_dwh_area_negocio_ctr,
     MAX_DATE_CGD_SEG_ADN md
where partition_date = md.max_date
and cod_jerq_adn01 = 'JAN01'
and cod_area_negocio_niv_9 <> 'null'
and cod_area_negocio_niv_9 <> cod_area_negocio_niv_8
union all
select
      '00011' attribute_code, cod_area_negocio_niv_10 dimension_value,des_area_negocio_niv_10 name,'00010' father_attribute_code,cod_area_negocio_niv_9 father_dimension_value,'9999-12-31' end_date
from bi_corp_staging.rio157_ms0_dm_dwh_area_negocio_ctr,
     MAX_DATE_CGD_SEG_ADN md
where partition_date = md.max_date
and cod_jerq_adn01 = 'JAN01'
and cod_area_negocio_niv_10 <> 'null'
and cod_area_negocio_niv_10 <> cod_area_negocio_niv_9
union all
select
      '00012' attribute_code, cod_area_negocio_niv_11 dimension_value,des_area_negocio_niv_11 name,'00011' father_attribute_code,cod_area_negocio_niv_10 father_dimension_value,'9999-12-31' end_date
from bi_corp_staging.rio157_ms0_dm_dwh_area_negocio_ctr,
     MAX_DATE_CGD_SEG_ADN md
where partition_date = md.max_date
and cod_jerq_adn01 = 'JAN01'
and cod_area_negocio_niv_11 <> 'null'
and cod_area_negocio_niv_11 <> cod_area_negocio_niv_10
union all
select distinct
      '00013' attribute_code, cod_area_negocio_niv_12 dimension_value,des_area_negocio_niv_12 name,'00012' father_attribute_code,cod_area_negocio_niv_11 father_dimension_value,'9999-12-31' end_date
from bi_corp_staging.rio157_ms0_dm_dwh_area_negocio_ctr,
     MAX_DATE_CGD_SEG_ADN md
where partition_date = md.max_date
and cod_jerq_adn01 = 'JAN01'
and cod_area_negocio_niv_12 <> 'null'
and cod_area_negocio_niv_12 <> cod_area_negocio_niv_11
union all
select
      '00014' attribute_code, cod_area_negocio_niv_13 dimension_value,des_area_negocio_niv_13 name,'00013' father_attribute_code,cod_area_negocio_niv_12 father_dimension_value,'9999-12-31' end_date
from bi_corp_staging.rio157_ms0_dm_dwh_area_negocio_ctr,
     MAX_DATE_CGD_SEG_ADN md
where partition_date = md.max_date
and cod_jerq_adn01 = 'JAN01'
and cod_area_negocio_niv_13 <> 'null'
and cod_area_negocio_niv_13 <> cod_area_negocio_niv_12
union all
select
      '00015' attribute_code, cod_area_negocio_niv_14 dimension_value,des_area_negocio_niv_14 name,'00014' father_attribute_code,cod_area_negocio_niv_13 father_dimension_value,'9999-12-31' end_date
from bi_corp_staging.rio157_ms0_dm_dwh_area_negocio_ctr,
     MAX_DATE_CGD_SEG_ADN md
where partition_date = md.max_date
and cod_jerq_adn01 = 'JAN01'
and cod_area_negocio_niv_14 <> 'null'
and cod_area_negocio_niv_14 <> cod_area_negocio_niv_13
)temp;

--traigo fecha de mdim
CREATE TEMPORARY TABLE IF NOT EXISTS MAX_DATE_CDG_MDIM as
select max(rm.partition_date) max_date
from bi_corp_common.rosetta_mdim rm,
	 bi_corp_staging.manual_rosetta_global_domains gd,
     bi_corp_staging.manual_rosetta_segmentation_global sg,
     MAX_DATE_GD mgd,
     MAX_DATE_GS mgs
where rm.partition_date <=  '{last_calendar_day}'
and   concat(rm.domain_code,rm.segmentation_code) = concat(gd.domain_code,sg.segmentation_code)
and   gd.domain_code = sg.domain_code
and   sg.name = 'Local Business Area Segmentation'
and   gd.second_name = 'Control de Gestion'
and   gd.partition_date = mgd.max_date
and   sg.partition_date = mgs.max_date;

--inserto tdim cdg
INSERT OVERWRITE TABLE bi_corp_common.rosetta_tdim
    PARTITION (domain_code,
               partition_date)
select
mdim.segmentation_code,
mdim.attribute_code,
trc.dimension_value,
'0' vision,
case when (trc.name is null or trc.name = 'null')
	   	then trc.dimension_value
	   else
	    trc.name
	   end name,
case when (trc.name is null or trc.name = 'null')
	   	then trc.dimension_value
	   else
	    trc.name
	   end second_name,
trc.father_attribute_code,
trc.father_dimension_value,
trc.end_date,
le.code unit_code,
mdim.domain_code,
'{last_calendar_day}' partition_date
from bi_corp_common.rosetta_mdim mdim,
     bi_corp_staging.manual_rosetta_segmentation_global sg,
     bi_corp_staging.manual_rosetta_legal_entity_unit_global le,
     TEMP_ROSETTA_CDG_SEGMENTATION trc,
     MAX_DATE_CDG_MDIM mcd,
     MAX_DATE_GS mgs,
     MAX_DATE_LE mle
where mdim.domain_code = sg.domain_code
and   mdim.segmentation_code = sg.segmentation_code
and   sg.name = 'Local Business Area Segmentation'
and   le.domain = 'UNIT GLOBAL'
and   trc.attribute_code = mdim.attribute_code
and   mdim.partition_date = mcd.max_date
and   sg.partition_date = mgs.max_date
and   le.partition_date = mle.max_date;