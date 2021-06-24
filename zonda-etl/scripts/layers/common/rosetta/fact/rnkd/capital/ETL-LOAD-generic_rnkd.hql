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
	   cast(cli.G4093_CLISEGM as int) G4093_CLISEGM
from bi_corp_bdr.jm_interv_cto rel,
	 bi_corp_bdr.jm_client_bii cli
where rel.partition_date = from_unixtime(unix_timestamp('{last_calendar_day}','yyyy-MM-dd'),'yyyy-MM')
and   rel.partition_date = cli.partition_date
and   rel.g4128_idnumcli = cli.g4093_idnumcli
and   rel.g4128_tipintev = '00001';

--tabla final para la segmentacion capital economico
CREATE TEMPORARY TABLE IF NOT EXISTS ECONOMIC_CAPITAL_SEGMENTATION as
select distinct
       g4095_contra1,
	   case when rel.G4093_TIPSEGL2 in ('A','B','C','S')
	   	then
	   		case when cast(ctr.G4095_ID_PROD as int) = 6
	   			 	then '1200009'
	   			 when cast(ctr.G4095_ID_PROD as int) in (13,182,183)
	   				then '1200041'
                 when cast(ctr.G4095_ID_PROD as int) = 17
                 	then '1200073'
                 when cast(ctr.G4095_ID_PROD as int) not in (17,6,13,182,183)
                 	then '1200105'
            end
        when rel.G4093_TIPSEGL2 in ('P1','C1')
        	then '1200137'
        when rel.G4093_TIPSEGL2 in ('P2')
			then '1200169'
		when rel.G4093_TIPSEGL2 in ('EM','G1','G2')
			then '1200201'
		when rel.G4093_TIPSEGL2 in ('IU','IP')
			then '1200233'
		when (rel.G4093_CLISEGM = 1 and rel.G4093_TIPSEGL2 in ('F2','OF','GL','LA','LO','MU','OT','FI','F1'))
			then '1040176'
		when (rel.G4093_CLISEGM = 11 and rel.G4093_TIPSEGL2 in ('F2','OF','GL','LA','LO','MU','OT','FI','F1'))
			then '1200290'
		when (rel.G4093_CLISEGM = 17 and rel.G4093_TIPSEGL2 in ('F2','OF','GL','LA','LO','MU','OT','FI','F1'))
			then '1040179'
		when (rel.G4093_CLISEGM in (8,2) and rel.G4093_TIPSEGL2 = 'PU' and ctr.g4095_coddiv = 'ARS')
			then '1200354'
		when (rel.G4093_CLISEGM in (8,2) and rel.G4093_TIPSEGL2 = 'PU' and ctr.g4095_coddiv <> 'ARS')
			then '1040181'
		end global_value
from bi_corp_bdr.jm_contr_bis ctr
	 left join REL_CONTRATO_CLIENTE rel
	 	on(ctr.g4095_contra1 = rel.g4128_contra1)
where ctr.partition_date = from_unixtime(unix_timestamp('{last_calendar_day}','yyyy-MM-dd'),'yyyy-MM');

--tabla final para la segmentacion std
CREATE TEMPORARY TABLE IF NOT EXISTS STD_SEGMENTATION as
select distinct
	ctr.g4095_contra1,
	case when cast(std.T0256_CATPRFIN as int) = 8 and cast(std.T0256_SBCPSFIN as int) = 0 and cast(std.T0256_INMORFIN as int) in (0,4) and cast(std.T0256_FLG_RIES as int) = 0
			then '002'
		 when cast(std.T0256_CATPRFIN as int) = 1 and cast(std.T0256_SBCPSFIN as int) = 0 and cast(std.T0256_INMORFIN as int) in (0,4) and cast(std.T0256_FLG_RIES as int) = 0
		 	then '003'
		 when cast(std.T0256_CATPRFIN as int) = 2 and cast(std.T0256_SBCPSFIN as int) = 0 and cast(std.T0256_INMORFIN as int) in (0,4) and cast(std.T0256_FLG_RIES as int) = 0
		 	then '004'
		 when cast(std.T0256_CATPRFIN as int) = 3 and cast(std.T0256_SBCPSFIN as int) = 0 and cast(std.T0256_INMORFIN as int) in (0,4) and cast(std.T0256_FLG_RIES as int) = 0
		 	then '005'
		 when cast(std.T0256_CATPRFIN as int) = 4 and cast(std.T0256_SBCPSFIN as int) = 0 and cast(std.T0256_INMORFIN as int) in (0,4) and cast(std.T0256_FLG_RIES as int) = 0
		 	then '006'
		 when cast(std.T0256_CATPRFIN as int) = 10 and cast(std.T0256_SBCPSFIN as int) = 0 and cast(std.T0256_INMORFIN as int) in (0,4) and cast(std.T0256_FLG_RIES as int) = 0
		 	then '007'
		 when cast(std.T0256_CATPRFIN as int) = 36 and cast(std.T0256_SBCPSFIN as int) = 0 and cast(std.T0256_INMORFIN as int) in (0,4) and cast(std.T0256_FLG_RIES as int) = 0
		 	then '008'
		 when cast(std.T0256_CATPRFIN as int) = 27 and cast(std.T0256_SBCPSFIN as int) = 0 and cast(std.T0256_INMORFIN as int) in (0,4) and cast(std.T0256_FLG_RIES as int) = 0
		 	then '009'
		 when cast(std.T0256_SBCPSFIN as int) in (4,5) and cast(std.T0256_INMORFIN as int) in (0,4) and cast(std.T0256_FLG_RIES as int) = 0
		 	then '010'
		 when cast(std.T0256_INMORFIN as int) in (1,2,3) and cast(std.T0256_FLG_RIES as int) = 0
		 	then '011'
		 when cast(std.T0256_FLG_RIES  as int) = 1
			then '012'
		 when cast(std.T0256_SBCPSFIN as int) = 1 and cast(std.T0256_INMORFIN as int) in (0,4) and cast(std.T0256_FLG_RIES as int) = 0
			then '013'
		 when cast(std.T0256_CATPRFIN as int) = 6 and cast(std.T0256_SBCPSFIN as int) = 0 and cast(std.T0256_INMORFIN as int) in (0,4) and cast(std.T0256_FLG_RIES as int) = 0
			then '014'
		 when cast(std.T0256_CATPRFIN as int) = 7 and cast(std.T0256_SBCPSFIN as int) = 0 and cast(std.T0256_INMORFIN as int) in (0,4) and cast(std.T0256_FLG_RIES as int) = 0
			then '015'
		 when cast(std.T0256_CATPRFIN as int) in (12,18,19,21,33) and cast(std.T0256_SBCPSFIN as int) = 0 and cast(std.T0256_INMORFIN as int) in (0,4) and cast(std.T0256_FLG_RIES as int) = 0
			then '017'
				else
					'016'
	end global_value
from bi_corp_bdr.jm_contr_bis ctr
	 left join bi_corp_bdr.jm_sim_aj_std std
	 	on(ctr.g4095_contra1 = std.t0256_contra1
	 	    and cast (std.T0256_FLG_CORE as bigint) = 1
	 		and ctr.partition_date = from_unixtime(unix_timestamp(std.partition_date ,'yyyy-MM-dd'),'yyyy-MM'))
where ctr.partition_date = from_unixtime(unix_timestamp('{last_calendar_day}','yyyy-MM-dd'),'yyyy-MM');

-- inserto capital
INSERT OVERWRITE TABLE bi_corp_common.rosetta_rnkd
PARTITION (domain_code)
select distinct
le.code,
ics.g4095_contra1,
sg.segmentation_code,
'00001' attribute_code,
null local_value,
ics.global_value,
sg.segmentation_type,
'9999-12-31' end_date,
'{last_calendar_day}' partition_date,
sg.domain_code
from ECONOMIC_CAPITAL_SEGMENTATION ics
	 inner join bi_corp_staging.manual_rosetta_legal_entity_unit_global le
        on (le.domain = 'LEGAL ENTITY')
	 inner join MAX_DATE_LE mle
	 	on(le.partition_date = mle.max_date)
	 inner join bi_corp_staging.manual_rosetta_segmentation_global sg
        on (sg.name = 'Economic Capital Segmentation')
	 inner join MAX_DATE_GS mgs
        on (sg.partition_date = mgs.max_date)
union all
select distinct
le.code,
std.g4095_contra1,
sg.segmentation_code,
'00001' attribute_code,
null local_value,
std.global_value,
sg.segmentation_type,
'9999-12-31' end_date,
'{last_calendar_day}' partition_date,
sg.domain_code
from STD_SEGMENTATION std
	 inner join bi_corp_staging.manual_rosetta_legal_entity_unit_global le
        on (le.domain = 'LEGAL ENTITY')
	 inner join MAX_DATE_LE mle
	 	on(le.partition_date = mle.max_date)
	 inner join bi_corp_staging.manual_rosetta_segmentation_global sg
        on (sg.name = 'STD Capital Segmentation')
	 inner join MAX_DATE_GS mgs
        on (sg.partition_date = mgs.max_date);