set hive.exec.dynamic.partition.mode=nonstrict;
set mapred.job.queue.name=root.dataeng;

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

--traigo maxima fecha de contratos mis
CREATE TEMPORARY TABLE IF NOT EXISTS MAX_DATE_MIS as
select max(partition_date) max_date
from bi_corp_staging.rio157_ms0_ft_dwh_blce_result
where partition_date >= trunc('{last_calendar_day}','MM')
and   partition_date <= '{last_calendar_day}';

--traigo maxima fecha de tabla de contratos ods
CREATE TEMPORARY TABLE IF NOT EXISTS MAX_DATE_CTR as
select max(data_date_part) max_date
from santander_business_risk_arda.cto_pasivo_mes_cta
where to_date(from_unixtime(UNIX_TIMESTAMP(data_date_part ,'yyyyMMdd'))) >= trunc('{last_calendar_day}','MM')
and   to_date(from_unixtime(UNIX_TIMESTAMP(data_date_part ,'yyyyMMdd'))) <= '{last_calendar_day}';

--armo todos los contratos a enviar ods
CREATE TEMPORARY TABLE IF NOT EXISTS ODS_CONTRATOS as
select distinct
	   t.cod_contenido,
	   t.idf_cto_ods
from (
select o.cod_contenido,
	   o.idf_cto_ods
from santander_business_risk_arda.cto_pasivo_mes_cta o,
     MAX_DATE_CTR md
where o.data_date_part = md.max_date
union all
select o.cod_contenido,
	   o.idf_cto_ods
from santander_business_risk_arda.cto_pasivo_mes_plz o,
     MAX_DATE_CTR md
where o.data_date_part = md.max_date
union all
select o.cod_contenido,
	   o.idf_cto_ods
from santander_business_risk_arda.cto_activo_mes_arf o,
     MAX_DATE_CTR md
where o.data_date_part = md.max_date
union all
select o.cod_contenido,
	   o.idf_cto_ods
from santander_business_risk_arda.cto_activo_mes_cco o,
     MAX_DATE_CTR md
where o.data_date_part = md.max_date
union all
select o.cod_contenido,
	   o.idf_cto_ods
from santander_business_risk_arda.cto_activo_mes_cre o,
     MAX_DATE_CTR md
where o.data_date_part = md.max_date
union all
select o.cod_contenido,
	   o.idf_cto_ods
from santander_business_risk_arda.cto_activo_mes_pre o,
     MAX_DATE_CTR md
where o.data_date_part = md.max_date
	 ) t;

--traigo todos los ids del mis
CREATE TEMPORARY TABLE IF NOT EXISTS MIS_CONTRATOS as
select distinct
r.idf_cto_ods,
r.cod_contenido,
r.cod_area_negocio,
r.cod_producto_gest
from   bi_corp_staging.rio157_ms0_ft_dwh_blce_result r,
       MAX_DATE_MIS mdm
where  r.partition_date = mdm.max_date
and    (r.imp_sdo_cap_ml	<> '0'
        or r.imp_sdo_cap_mo	<> '0'
        or r.imp_sdo_int_ml	<> '0'
        or r.imp_sdo_int_mo	<> '0'
        or r.imp_sdo_reajuste_ml <> '0'
        or r.imp_sdo_reajuste_mo <> '0'
        or r.imp_sdo_insolv_ml	<> '0'
        or r.imp_sdo_insolv_mo	<> '0'
        or r.imp_sdo_cap_med_ml	<> '0'
        or r.imp_sdo_cap_med_mo	<> '0'
        or r.imp_sdo_med_int_ml	<> '0'
        or r.imp_sdo_med_int_mo	<> '0'
        or r.imp_sdo_med_reajuste_ml <> '0'
        or r.imp_sdo_med_reajuste_mo <> '0'
        or r.imp_sdo_med_insolv_ml	<> '0'
        or r.imp_sdo_med_insolv_mo	<> '0'
        or r.imp_egr_cap_ml	<> '0'
        or r.imp_egr_cap_mo	<> '0'
        or r.imp_egr_per_ml	<> '0'
        or r.imp_egr_per_mo	<> '0'
        or r.imp_egr_reaj_ml <> '0'
        or r.imp_egr_reaj_mo <> '0'
        or r.imp_ing_cap_ml	<> '0'
        or r.imp_ing_cap_mo	<> '0'
        or r.imp_ing_per_ml	<> '0'
        or r.imp_ing_per_mo	<> '0'
        or r.imp_ing_reaj_ml <> '0'
        or r.imp_ing_reaj_mo <> '0'
        or r.imp_ajtti_egr_tb_cap_ml <> '0'
        or r.imp_ajtti_egr_tb_cap_mo <> '0'
        or r.imp_ajtti_egr_sl_cap_ml <> '0'
        or r.imp_ajtti_egr_sl_cap_mo <> '0'
        or r.imp_ajtti_egr_per_ml <> '0'
        or r.imp_ajtti_egr_per_mo <> '0'
        or r.imp_ajtti_egr_reajuste_ml	<> '0'
        or r.imp_ajtti_egr_reajuste_mo	<> '0'
        or r.imp_ajtti_ing_tb_cap_ml <> '0'
        or r.imp_ajtti_ing_tb_cap_mo <> '0'
        or r.imp_ajtti_ing_sl_cap_ml <> '0'
        or r.imp_ajtti_ing_sl_cap_mo <> '0'
        or r.imp_ajtti_ing_per_ml	<> '0'
        or r.imp_ajtti_ing_per_mo	<> '0'
        or r.imp_ajtti_ing_reajuste_ml	<> '0'
        or r.imp_ajtti_ing_reajuste_mo	<> '0'
        or r.imp_efec_enc_ml <> '0'
        or r.imp_efec_enc_mo <> '0');

--armo tabla final de segmentacion con los contrados de ODS que existen en MIS_CONTRATOS
CREATE TEMPORARY TABLE IF NOT EXISTS CDG_CONTRATOS as
select distinct
	   concat(o.cod_contenido,
			  o.idf_cto_ods) native_key,
	   m.cod_area_negocio,
	   m.cod_producto_gest
from ODS_CONTRATOS o,
	 MIS_CONTRATOS m
where o.idf_cto_ods = m.idf_cto_ods
and   o.cod_contenido = m.cod_contenido;

--traigo maxima fecha de la tabla segmentacion global CDG
CREATE TEMPORARY TABLE IF NOT EXISTS MAX_DATE_CDG_TDIM as
select max(rm.partition_date) max_date
from bi_corp_common.rosetta_mdim rm,
	 bi_corp_staging.manual_rosetta_global_domains gd,
     bi_corp_staging.manual_rosetta_segmentation_global sg,
     MAX_DATE_GD mgd,
     MAX_DATE_GS mgs
where rm.partition_date <= '{last_calendar_day}'
and   concat(rm.domain_code,rm.segmentation_code) = concat(gd.domain_code,sg.segmentation_code)
and   gd.domain_code = sg.domain_code
and   sg.name = 'Local Business Area Segmentation'
and   gd.second_name = 'Control de Gestion'
and   gd.partition_date = mgd.max_date
and   sg.partition_date = mgs.max_date;

--traigo maxima fecha de la tabla segmentacion global
CREATE TEMPORARY TABLE IF NOT EXISTS MAX_DATE_SEG_GBL as
select max(j.partition_date) max_date
from bi_corp_staging.rio157_ms0_dm_dwh_composicion_jerqs j
where j.COD_PAIS = '54'
and   j.partition_date >= trunc('{last_calendar_day}','MM')
and   j.partition_date <= '{last_calendar_day}';

--armo dimensiones globales ADN y producto
CREATE TEMPORARY TABLE IF NOT EXISTS SEG_GLB as
select  distinct
		jl.cod_valor valor_local,
		jg.cod_valor valor_global,
		lpad(jg.num_nivel,5,'0') nivel,
		jg.cod_jerarquia origen
from    bi_corp_staging.rio157_ms0_dm_dwh_composicion_jerqs jl,
		bi_corp_staging.rio157_ms0_dm_dwh_composicion_jerqs jg,
		MAX_DATE_SEG_GBL mdg
where   jl.partition_date = mdg.max_date
and		jl.cod_pais = '54'
and     jl.cod_jerarquia in ('JAN01','JBLDN')
and     jl.tip_elemento = 'L'
and     jg.partition_date = mdg.max_date
and		jg.cod_pais = '54'
and     jg.cod_jerarquia in ('JAN01','JBLDN')
and     jg.tip_elemento = 'C'
and     jl.primer_padre_corp = jg.cod_valor
and     jl.cod_jerarquia = jg.cod_jerarquia;

--armo tdim con la fecha de analisis solo para CDG
CREATE TEMPORARY TABLE IF NOT EXISTS CDG_TDIM as
select rt.segmentation_code,
	   rt.attribute_code,
	   rt.dimension_value,
	   rt.name
from bi_corp_common.rosetta_tdim rt,
	 bi_corp_staging.manual_rosetta_global_domains gd,
     bi_corp_staging.manual_rosetta_segmentation_global sg,
     MAX_DATE_GD mgd,
     MAX_DATE_GS mgs,
     MAX_DATE_CDG_TDIM ct
where rt.partition_date <= '{last_calendar_day}'
and   concat(rt.domain_code,rt.segmentation_code) = concat(gd.domain_code,sg.segmentation_code)
and   gd.domain_code = sg.domain_code
and   sg.name = 'Local Business Area Segmentation'
and   gd.second_name = 'Control de Gestion'
and   gd.partition_date = mgd.max_date
and   sg.partition_date = mgs.max_date
and   rt.partition_date = ct.max_date;

--inserto rnkd cdg
INSERT OVERWRITE TABLE bi_corp_common.rosetta_rnkd
PARTITION (domain_code)
select distinct
le.code,
cdg.native_key,
sg.segmentation_code,
tdim.attribute_code,
null local_value,
tdim.dimension_value global_value,
sg.segmentation_type,
'9999-12-31' end_date,
'{last_calendar_day}' partition_date,
sg.domain_code
from CDG_CONTRATOS cdg
	inner join bi_corp_staging.manual_rosetta_legal_entity_unit_global le
    	on (le.domain = 'LEGAL ENTITY')
    inner join MAX_DATE_LE mle
	 	on (le.partition_date = mle.max_date)
    inner join bi_corp_staging.manual_rosetta_segmentation_global sg
        on (sg.name = 'Local Business Area Segmentation')
    inner join MAX_DATE_GS mgs
        on (sg.partition_date = mgs.max_date)
    left join CDG_TDIM tdim
        on (cdg.cod_area_negocio = tdim.dimension_value)
union all
select distinct
le.code,
cdg.native_key,
sg.segmentation_code,
glb.nivel attribute_code,
null local_value,
glb.valor_global global_value,
sg.segmentation_type,
'9999-12-31' end_date,
'{last_calendar_day}' partition_date,
sg.domain_code
from CDG_CONTRATOS cdg
	inner join bi_corp_staging.manual_rosetta_legal_entity_unit_global le
    	on (le.domain = 'LEGAL ENTITY')
    inner join MAX_DATE_LE mle
	 	on (le.partition_date = mle.max_date)
    inner join bi_corp_staging.manual_rosetta_segmentation_global sg
        on (sg.name = 'Business Area Segmentation')
    inner join MAX_DATE_GS mgs
        on (sg.partition_date = mgs.max_date)
    left join SEG_GLB glb
        on (glb.origen = 'JAN01'
            	and cdg.cod_area_negocio = glb.valor_local)
union all
select distinct
le.code,
cdg.native_key,
sg.segmentation_code,
glb.nivel attribute_code,
null local_value,
glb.valor_global global_value,
sg.segmentation_type,
'9999-12-31' end_date,
'{last_calendar_day}' partition_date,
sg.domain_code
from CDG_CONTRATOS cdg
	inner join bi_corp_staging.manual_rosetta_legal_entity_unit_global le
    	on (le.domain = 'LEGAL ENTITY')
    inner join MAX_DATE_LE mle
	 	on (le.partition_date = mle.max_date)
    inner join bi_corp_staging.manual_rosetta_segmentation_global sg
        on (sg.name = 'Product')
    inner join MAX_DATE_GS mgs
        on (sg.partition_date = mgs.max_date)
    left join SEG_GLB glb
        on (glb.origen = 'JBLDN'
            	and cdg.cod_producto_gest = glb.valor_local)
union all
select distinct
le.code,
cdg.native_key,
sg.segmentation_code,
'00003' attribute_code,
null local_value,
'22150' global_value,
sg.segmentation_type,
'9999-12-31' end_date,
'{last_calendar_day}' partition_date,
sg.domain_code
from CDG_CONTRATOS cdg
	inner join bi_corp_staging.manual_rosetta_legal_entity_unit_global le
    	on (le.domain = 'LEGAL ENTITY')
    inner join MAX_DATE_LE mle
	 	on (le.partition_date = mle.max_date)
    inner join bi_corp_staging.manual_rosetta_segmentation_global sg
        on (sg.name = 'Geography')
    inner join MAX_DATE_GS mgs
        on (sg.partition_date = mgs.max_date);