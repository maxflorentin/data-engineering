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

--traigo relacion cliente/contrato
CREATE TEMPORARY TABLE IF NOT EXISTS REL_CONTRATO_CLIENTE as
select trim(rel.g4128_contra1)  G4128_contra1,
	   trim(cli.g4093_idnumcli) G4093_idnumcli,
	   trim(cli.G4093_TIPSEGL2) G4093_TIPSEGL2,
	   cast(cli.G4093_CLISEGM as int) G4093_CLISEGM,
	   cast(otr.E0623_GEST_SIT as int) E0623_GEST_SIT
from bi_corp_bdr.jm_interv_cto rel,
	 bi_corp_bdr.jm_client_bii cli,
	 bi_corp_bdr.jm_contr_otros otr
where rel.partition_date = from_unixtime(unix_timestamp('{last_calendar_day}','yyyy-MM-dd'),'yyyy-MM')
and   rel.partition_date = cli.partition_date
and   rel.g4128_idnumcli = cli.g4093_idnumcli
and   rel.g4128_tipintev = '00001'
and   rel.partition_date  = otr.partition_date
and   rel.g4128_contra1  = otr.e0623_contra1;

--tabla final para la segmentacion
CREATE TEMPORARY TABLE IF NOT EXISTS ICAAP_SEGMENTATION as
select distinct
       g4095_contra1,
	   case when rel.G4093_TIPSEGL2 in ('A','B','C','S')
	   	then
	   		case when cast(ctr.G4095_ID_PROD as int) = 6
	   			 	then 'Retail Credit Cards'
	   			 when cast(ctr.G4095_ID_PROD as int) in (13,182,183)
	   				then 'Retail Consumer'
                 when cast(ctr.G4095_ID_PROD as int) = 17
                 	then 'Retail Mortgages'
                 when cast(ctr.G4095_ID_PROD as int) not in (17,6,13,182,183)
                 	then 'Retail Consumer Other'
            end
        when rel.G4093_TIPSEGL2 in ('P1','C1')
        	then 'SME I'
        when rel.G4093_TIPSEGL2 in ('P2')
			then 'SME II'
		when rel.G4093_TIPSEGL2 in ('EM','G1','G2')
			then 'Non Global Corporate'
		when rel.G4093_TIPSEGL2 in ('IU','IP')
			then 'Public sector Institutions'
		when (rel.G4093_CLISEGM = 1 and rel.G4093_TIPSEGL2 in ('F2','OF','GL','LA','LO','MU','OT','FI','F1'))
			then 'Global Corporate'
		when (rel.G4093_CLISEGM = 11 and rel.G4093_TIPSEGL2 in ('F2','OF','GL','LA','LO','MU','OT','FI','F1'))
			then 'Banks'
		when (rel.G4093_CLISEGM = 17 and rel.G4093_TIPSEGL2 in ('F2','OF','GL','LA','LO','MU','OT','FI','F1'))
			then 'Other Financial Institutions'
		when (rel.G4093_CLISEGM in (8,2) and rel.G4093_TIPSEGL2 = 'PU' and ctr.g4095_coddiv = 'ARS')
			then 'Sovereign Local Currency'
		when (rel.G4093_CLISEGM in (8,2) and rel.G4093_TIPSEGL2 = 'PU' and ctr.g4095_coddiv <> 'ARS')
			then 'Soberanos moneda extranjera'
		end parametro,
		case when rel.G4093_TIPSEGL2 in ('A','B','C','S')
	   	then
	   		case when cast(ctr.G4095_ID_PROD as int) = 6 and rel.E0623_GEST_SIT in (13 ,15 ,5 ,6 ,7 ,8 ,9 ,18, 24, 25)
	   			 	then '4.2'
	   			 when cast(ctr.G4095_ID_PROD as int) = 6 and rel.E0623_GEST_SIT not in (13 ,15 ,5 ,6 ,7 ,8 ,9 ,18, 24, 25)
	   			 	then '3.2'
	   			 when cast(ctr.G4095_ID_PROD as int) in (13,182,183) and rel.E0623_GEST_SIT in (13 ,15 ,5 ,6 ,7 ,8 ,9 ,18, 24, 25)
	   				then '4.3'
	   			 when cast(ctr.G4095_ID_PROD as int) in (13,182,183) and rel.E0623_GEST_SIT not in (13 ,15 ,5 ,6 ,7 ,8 ,9 ,18, 24, 25)
	   			 	then '3.3'
                 when cast(ctr.G4095_ID_PROD as int) = 17 and rel.E0623_GEST_SIT in (13 ,15 ,5 ,6 ,7 ,8 ,9 ,18, 24, 25)
                 	then '4.1'
                 when cast(ctr.G4095_ID_PROD as int) = 17 and rel.E0623_GEST_SIT not in (13 ,15 ,5 ,6 ,7 ,8 ,9 ,18, 24, 25)
                 	then '3.1'
                 when cast(ctr.G4095_ID_PROD as int) not in (17,6,13,82,183) and rel.E0623_GEST_SIT in (13 ,15 ,5 ,6 ,7 ,8 ,9 ,18, 24, 25)
                 	then '4.3'
                 when cast(ctr.G4095_ID_PROD as int) not in (17,6,13,82,183) and rel.E0623_GEST_SIT not in (13 ,15 ,5 ,6 ,7 ,8 ,9 ,18, 24, 25)
                 	then '3.3'
            end
        when rel.G4093_TIPSEGL2 in ('P1','C1') and rel.E0623_GEST_SIT in (13 ,15 ,5 ,6 ,7 ,8 ,9 ,18, 24, 25)
        	then '4.4'
        when rel.G4093_TIPSEGL2 in ('P1','C1') and rel.E0623_GEST_SIT not in (13 ,15 ,5 ,6 ,7 ,8 ,9 ,18, 24, 25)
        	then '3.4'
        when rel.G4093_TIPSEGL2 in ('P2') and rel.E0623_GEST_SIT in (13 ,15 ,5 ,6 ,7 ,8 ,9 ,18, 24, 25)
			then '4.5'
		when rel.G4093_TIPSEGL2 in ('P2') and rel.E0623_GEST_SIT  not in (13 ,15 ,5 ,6 ,7 ,8 ,9 ,18, 24, 25)
			then '3.5'
		when rel.G4093_TIPSEGL2 in ('EM','G1','G2') and rel.E0623_GEST_SIT in (13 ,15 ,5 ,6 ,7 ,8 ,9 ,18, 24, 25)
			then '4.6'
		when rel.G4093_TIPSEGL2 in ('EM','G1','G2') and rel.E0623_GEST_SIT not in (13 ,15 ,5 ,6 ,7 ,8 ,9 ,18, 24, 25)
			then '3.6'
		when rel.G4093_TIPSEGL2 in ('IU','IP') and rel.E0623_GEST_SIT in (13 ,15 ,5 ,6 ,7 ,8 ,9 ,18, 24, 25)
			then '4.9'
		when rel.G4093_TIPSEGL2 in ('IU','IP') and rel.E0623_GEST_SIT not in (13 ,15 ,5 ,6 ,7 ,8 ,9 ,18, 24, 25)
			then '3.9'
		when rel.G4093_CLISEGM = 1 and rel.G4093_TIPSEGL2 in ('F2','OF','GL','LA','LO','MU','PU','OT','FI','F1') and rel.E0623_GEST_SIT in (13 ,15 ,5 ,6 ,7 ,8 ,9 ,18, 24, 25)
			then '4.7'
		when rel.G4093_CLISEGM = 1 and rel.G4093_TIPSEGL2 in ('F2','OF','GL','LA','LO','MU','PU','OT','FI','F1') and rel.E0623_GEST_SIT not in (13 ,15 ,5 ,6 ,7 ,8 ,9 ,18, 24, 25)
			then '3.7'
		when rel.G4093_CLISEGM = 11 and rel.G4093_TIPSEGL2 in ('F2','OF','GL','LA','LO','MU','PU','OT','FI','F1') and rel.E0623_GEST_SIT in (13 ,15 ,5 ,6 ,7 ,8 ,9 ,18, 24, 25)
			then '4.9'
		when rel.G4093_CLISEGM = 11 and rel.G4093_TIPSEGL2 in ('F2','OF','GL','LA','LO','MU','PU','OT','FI','F1') and rel.E0623_GEST_SIT not in (13 ,15 ,5 ,6 ,7 ,8 ,9 ,18, 24, 25)
			then '3.9'
		when rel.G4093_CLISEGM = 17 and rel.G4093_TIPSEGL2 in ('F2','OF','GL','LA','LO','MU','PU','OT','FI','F1') and rel.E0623_GEST_SIT in (13 ,15 ,5 ,6 ,7 ,8 ,9 ,18, 24, 25)
			then '4.9'
		when rel.G4093_CLISEGM = 17 and rel.G4093_TIPSEGL2 in ('F2','OF','GL','LA','LO','MU','PU','OT','FI','F1') and rel.E0623_GEST_SIT not in (13 ,15 ,5 ,6 ,7 ,8 ,9 ,18, 24, 25)
			then '3.9'
		when rel.G4093_CLISEGM in (8,2) and rel.G4093_TIPSEGL2 = 'PU' and ctr.g4095_coddiv = 'ARS' and rel.E0623_GEST_SIT in (13 ,15 ,5 ,6 ,7 ,8 ,9 ,18, 24, 25)
			then '4.10'
		when rel.G4093_CLISEGM in (8,2) and rel.G4093_TIPSEGL2 = 'PU' and ctr.g4095_coddiv = 'ARS' and rel.E0623_GEST_SIT not in (13 ,15 ,5 ,6 ,7 ,8 ,9 ,18, 24, 25)
			then '3.10'
		when rel.G4093_CLISEGM in (8,2) and rel.G4093_TIPSEGL2 = 'PU' and ctr.g4095_coddiv <> 'ARS' and rel.E0623_GEST_SIT in (13 ,15 ,5 ,6 ,7 ,8 ,9 ,18, 24, 25)
			then '4.10'
		when rel.G4093_CLISEGM in (8,2) and rel.G4093_TIPSEGL2 = 'PU' and ctr.g4095_coddiv <> 'ARS' and rel.E0623_GEST_SIT not in (13 ,15 ,5 ,6 ,7 ,8 ,9 ,18, 24, 25)
			then '3.10'
		end global_value
from bi_corp_bdr.jm_contr_bis ctr
	 left join REL_CONTRATO_CLIENTE rel
	 	on(ctr.g4095_contra1 = rel.g4128_contra1)
where ctr.partition_date = from_unixtime(unix_timestamp('{last_calendar_day}','yyyy-MM-dd'),'yyyy-MM');

--traigo fecha de tdim icaap
CREATE TEMPORARY TABLE IF NOT EXISTS MAX_DATE_ICAAP_TDIM as
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

--armo tdim con la fecha de analisis solo para ICAAP
CREATE TEMPORARY TABLE IF NOT EXISTS ICAAP_TDIM as
select rt.segmentation_code,
	   rt.attribute_code,
	   rt.dimension_value,
	   rt.name
from bi_corp_common.rosetta_tdim rt,
	 bi_corp_staging.manual_rosetta_global_domains gd,
     bi_corp_staging.manual_rosetta_segmentation_global sg,
     MAX_DATE_GD mgd,
     MAX_DATE_GS mgs,
     MAX_DATE_ICAAP_TDIM mt
where rt.partition_date <=  '{last_calendar_day}'
and   concat(rt.domain_code,rt.segmentation_code) = concat(gd.domain_code,sg.segmentation_code)
and   gd.domain_code = sg.domain_code
and   sg.name = 'ICAAP Segmentation (Local)'
and   gd.second_name = 'ICAAP'
and   gd.partition_date = mgd.max_date
and   sg.partition_date = mgs.max_date
and   rt.partition_date = mt.max_date;

-- inserto icaap
INSERT OVERWRITE TABLE bi_corp_common.rosetta_rnkd
PARTITION (domain_code)
select distinct
le.code,
ics.g4095_contra1 ,
sg.segmentation_code,
tdim.attribute_code,
null local_value,
tdim.dimension_value global_value,
sg.segmentation_type,
'9999-12-31' end_date,
'{last_calendar_day}' partition_date,
sg.domain_code
from ICAAP_SEGMENTATION ics
	 inner join bi_corp_staging.manual_rosetta_legal_entity_unit_global le
        on (le.domain = 'LEGAL ENTITY')
	 inner join MAX_DATE_LE mle
	 	on (le.partition_date = mle.max_date)
	 inner join bi_corp_staging.manual_rosetta_segmentation_global sg
        on (sg.name = 'ICAAP Segmentation (Local)')
	 inner join MAX_DATE_GS mgs
        on (sg.partition_date = mgs.max_date)
    left join ICAAP_TDIM tdim
        on (ics.parametro = tdim.name)
union all
select distinct
le.code,
ics.g4095_contra1 ,
sg.segmentation_code,
'00002' attribute_code,
null local_value,
ics.global_value,
sg.segmentation_type,
'9999-12-31' end_date,
'{last_calendar_day}' partition_date,
sg.domain_code
from ICAAP_SEGMENTATION ics
	 inner join bi_corp_staging.manual_rosetta_legal_entity_unit_global le
        on (le.domain = 'LEGAL ENTITY')
	 inner join MAX_DATE_LE mle
	 	on (le.partition_date = mle.max_date)
	 inner join bi_corp_staging.manual_rosetta_segmentation_global sg
        on (sg.name = 'ICAAP Segmentation (Corp)')
	 inner join MAX_DATE_GS mgs
        on (sg.partition_date = mgs.max_date);