set hive.exec.dynamic.partition.mode=nonstrict;
set mapred.job.queue.name=root.dataeng;

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

--traigo maxima fecha de normalizacion de productos
CREATE TEMPORARY TABLE IF NOT EXISTS MAX_NORM_PROD as
select max(data_date_part) max_date
from santander_business_risk_arda.normaliz_productos
where to_date(from_unixtime(UNIX_TIMESTAMP(data_date_part ,'yyyyMMdd'))) <= '{last_calendar_day}';

--traigo todas las combinaciones unicas de cada prod/subprod normalizadas
CREATE TEMPORARY TABLE IF NOT EXISTS NORM_SUBP as
SELECT distinct
	  p.cod_producto,
	  p.cod_subprodu,
	  p.cod_subprodu_operac
FROM santander_business_risk_arda.normaliz_productos p,
     MAX_NORM_PROD m
     where p.data_date_part = m.max_date
     and p.cod_subprodu_operac not in ('0STD','6971');

--traigo maxima fecha de tabla de contratos ods
CREATE TEMPORARY TABLE IF NOT EXISTS MAX_DATE_CTR as
select max(data_date_part) max_date
from santander_business_risk_arda.cto_pasivo_mes_cta
where to_date(from_unixtime(UNIX_TIMESTAMP(data_date_part ,'yyyyMMdd'))) >= trunc('{last_calendar_day}','MM')
and   to_date(from_unixtime(UNIX_TIMESTAMP(data_date_part ,'yyyyMMdd'))) <= '{last_calendar_day}';

--traigo maxima fecha de contratos mis
CREATE TEMPORARY TABLE IF NOT EXISTS MAX_DATE_MIS as
select max(partition_date) max_date
from bi_corp_staging.rio157_ms0_ft_dwh_blce_result
where partition_date >= trunc('{last_calendar_day}','MM')
and   partition_date <= '{last_calendar_day}';

--armo todos los contratos a enviar ods
CREATE TEMPORARY TABLE IF NOT EXISTS ODS_CONTRATOS as
select distinct
	   t.cod_contenido,
	   t.idf_cto_ods,
	   t.cod_entidad,
	   t.cod_centro,
	   t.num_cuenta,
	   t.cod_producto,
	   nvl(n.cod_subprodu_operac,t.cod_subprodu) cod_subprodu
from (
select o.cod_contenido,
	   o.idf_cto_ods,
	   o.cod_entidad,
	   o.cod_centro,
	   o.num_cuenta,
	   o.cod_producto,
	   o.cod_subprodu
from santander_business_risk_arda.cto_pasivo_mes_cta o,
     MAX_DATE_CTR md
where o.data_date_part = md.max_date
union all
select o.cod_contenido,
	   o.idf_cto_ods,
	   o.cod_entidad,
	   o.cod_centro,
	   o.num_cuenta,
	   o.cod_producto,
	   o.cod_subprodu
from santander_business_risk_arda.cto_pasivo_mes_plz o,
     MAX_DATE_CTR md
where o.data_date_part = md.max_date
union all
select o.cod_contenido,
	   o.idf_cto_ods,
	   o.cod_entidad,
	   o.cod_centro,
	   o.num_cuenta,
	   o.cod_producto,
	   o.cod_subprodu
from santander_business_risk_arda.cto_activo_mes_arf o,
     MAX_DATE_CTR md
where o.data_date_part = md.max_date
union all
select o.cod_contenido,
	   o.idf_cto_ods,
	   o.cod_entidad,
	   o.cod_centro,
	   o.num_cuenta,
	   o.cod_producto,
	   o.cod_subprodu
from santander_business_risk_arda.cto_activo_mes_cco o,
     MAX_DATE_CTR md
where o.data_date_part = md.max_date
union all
select o.cod_contenido,
	   o.idf_cto_ods,
	   o.cod_entidad,
	   o.cod_centro,
	   o.num_cuenta,
	   o.cod_producto,
	   o.cod_subprodu
from santander_business_risk_arda.cto_activo_mes_cre o,
     MAX_DATE_CTR md
where o.data_date_part = md.max_date
union all
select o.cod_contenido,
	   o.idf_cto_ods,
	   o.cod_entidad,
	   o.cod_centro,
	   o.num_cuenta,
	   o.cod_producto,
	   o.cod_subprodu
from santander_business_risk_arda.cto_activo_mes_pre o,
     MAX_DATE_CTR md
where o.data_date_part = md.max_date
	 ) t
	  left join NORM_SUBP n
		on  ( t.cod_producto =  n.cod_producto
			and   t.cod_subprodu =  n.cod_subprodu
    );

--traigo todos los ids del mis
CREATE TEMPORARY TABLE IF NOT EXISTS MIS_CONTRATOS as
select distinct
r.idf_cto_ods,
r.cod_contenido
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
select o.cod_contenido,
	   o.idf_cto_ods,
	   o.cod_entidad,
	   o.cod_centro,
	   case when o.cod_producto in ('05','06','07')
	   	then
	   		concat('000',substr(o.num_cuenta,4))
	   	else
	   		o.num_cuenta
	   end num_cuenta,
	   o.cod_producto,
	   o.cod_subprodu
from ODS_CONTRATOS o,
	 MIS_CONTRATOS m
where o.idf_cto_ods = m.idf_cto_ods
and   o.cod_contenido = m.cod_contenido;

-- inserto nkey control de gestion
INSERT OVERWRITE TABLE bi_corp_common.rosetta_nkey
PARTITION (domain_code)
select  distinct
        le.code legal_entity_code,
		concat(cc.cod_contenido,
			   cc.idf_cto_ods) native_key,
		concat(cc.cod_entidad,
               cc.cod_centro,
               cc.num_cuenta,
               cc.cod_producto,
               cc.cod_subprodu
               ) master_key,
		'9999-12-31' end_date,
		'{last_calendar_day}' partition_date,
		 gd.domain_code
from CDG_CONTRATOS cc,
	 bi_corp_staging.manual_rosetta_global_domains gd,
     bi_corp_staging.manual_rosetta_legal_entity_unit_global le,
     MAX_DATE_GD mgd,
     MAX_DATE_LE mle
where gd.second_name = 'Control de Gestion'
and   le.domain = 'LEGAL ENTITY'
and   gd.partition_date = mgd.max_date
and   le.partition_date = mle.max_date;