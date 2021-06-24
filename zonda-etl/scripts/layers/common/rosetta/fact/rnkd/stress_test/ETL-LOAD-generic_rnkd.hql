set hive.exec.dynamic.partition.mode=nonstrict;
set mapred.job.queue.name=root.dataeng;

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

--tabla final para la segmentacion
CREATE TEMPORARY TABLE IF NOT EXISTS STEST_SEGMENTATION as
select distinct
       std.T0256_CONTRA1,
	   case when cast(std.T0256_CATPRFIN  as int) = 8 and cast(std.T0256_SBCPSFIN as int) = 0 and cast(std.T0256_FLG_RIES as int) = 0
	        then '3.1'
        when cast(std.T0256_CATPRFIN  as int) = 1 and cast(std.T0256_SBCPSFIN as int) = 0 and cast(std.T0256_FLG_RIES as int) = 0
		 	then '3.3'
        when cast(std.T0256_CATPRFIN  as int) = 2 and cast(std.T0256_SBCPSFIN as int) = 0 and cast(std.T0256_FLG_RIES as int) = 0
		 	then '3.4'
        when cast(std.T0256_CATPRFIN  as int) = 3 and cast(std.T0256_SBCPSFIN as int) = 0 and cast(std.T0256_FLG_RIES as int) = 0
		 	then '3.5'
        when cast(std.T0256_CATPRFIN  as int) = 4 and cast(std.T0256_SBCPSFIN as int) = 0 and cast(std.T0256_FLG_RIES as int) = 0
		 	then '3.6'
        when cast(std.T0256_CATPRFIN  as int) = 10 and cast(std.T0256_SBCPSFIN as int) = 0 and cast(std.T0256_FLG_RIES as int) = 0
            then '3.7'
        when cast(std.T0256_CATPRFIN  as int) = 36 and cast(std.T0256_SBCPSFIN as int) = 0 and cast(std.T0256_FLG_RIES as int) = 0 and cast(vr.R9451_FLG_PYME as int) = 1
            then '3.8.1'
		when cast(std.T0256_CATPRFIN  as int) = 36 and cast(std.T0256_SBCPSFIN as int) = 0 and cast(std.T0256_FLG_RIES as int) = 0 and cast(vr.R9451_FLG_PYME as int) = 0
            then '3.8.2'
        when cast(std.T0256_CATPRFIN  as int) = 27 and cast(std.T0256_SBCPSFIN as int) = 0 and cast(std.T0256_FLG_RIES as int) = 0 and cast(vr.R9451_FLG_PYME as int) = 1
            then '3.9.1'
        when cast(std.T0256_CATPRFIN  as int) = 27 and cast(std.T0256_SBCPSFIN as int) = 0 and cast(std.T0256_FLG_RIES as int) = 0 and cast(vr.R9451_FLG_PYME as int) = 0
            then '3.9.2'
        when cast(std.T0256_SBCPSFIN as int) in (4,5) and cast(std.T0256_FLG_RIES as int) = 0 and cast(vr.R9451_FLG_PYME as int) = 1
            then '3.10.1'
        when cast(std.T0256_SBCPSFIN as int) in (4,5) and cast(std.T0256_FLG_RIES as int) = 0 and cast(vr.R9451_FLG_PYME as int) = 0
            then '3.10.2'
        when cast(std.T0256_FLG_RIES as int) = 1
            then '3.11'
        when cast(std.T0256_SBCPSFIN as int) = 1 and cast(std.T0256_FLG_RIES as int) = 0
            then '3.12'
        when cast(std.T0256_CATPRFIN  as int) = 6 and cast(std.T0256_SBCPSFIN as int) = 0 and cast(std.T0256_FLG_RIES as int) = 0
            then '3.13'
        when cast(std.T0256_CATPRFIN  as int) = 7 and cast(std.T0256_SBCPSFIN as int) = 0 and cast(std.T0256_FLG_RIES as int) = 0
            then '3.14'
        when cast(std.T0256_CATPRFIN  as int) in (12,18,19,21,33) and cast(std.T0256_SBCPSFIN as int) = 0 and cast(std.T0256_FLG_RIES as int) = 0
            then '3.17'
        end global_value
from bi_corp_bdr.jm_sim_aj_std std
	 left join bi_corp_bdr.normalizacion_id_contratos nor
	 	on (std.t0256_contra1 = nor.id_cto_bdr
	 	    and from_unixtime(unix_timestamp(std.partition_date,'yyyy-MM-dd'),'yyyy-MM') = nor.partition_date)
	 left join bi_corp_bdr.jm_prov_esotr es
	 	on (es.n0625_contra1 = nor.id_cto_bdr
	 		and es.partition_date = nor.partition_date)
	 left join bi_corp_bdr.jm_vr_cli_std vr
	 	on (std.T0256_idnumcli = vr.r9451_idnumcli
	 		and std.partition_date = vr.partition_date)
where trunc(std.partition_date,'MM') = trunc('{last_calendar_day}','MM')
and cast (std.t0256_flg_core as bigint) = 1
and substr(es.n0625_importh,1,1) = '+'
and cast(substr(es.n0625_importh,2) as bigint) > 0;

-- inserto stres_test
INSERT OVERWRITE TABLE bi_corp_common.rosetta_rnkd
PARTITION (domain_code)
select distinct
le.code,
st.T0256_contra1,
sg.segmentation_code,
case when st.global_value in ('3.8.1','3.8.2','3.9.1','3.9.2','3.10.1','3.10.2')
        then '00003'
     when st.global_value is null
        then null
            else '00002'
end attribute_code,
null local_value,
st.global_value,
sg.segmentation_type,
'9999-12-31' end_date,
'{last_calendar_day}' partition_date,
sg.domain_code
from STEST_SEGMENTATION st
	 inner join bi_corp_staging.manual_rosetta_legal_entity_unit_global le
        on (le.domain = 'LEGAL ENTITY')
	 inner join MAX_DATE_LE mle
	 	on (le.partition_date = mle.max_date)
	 inner join bi_corp_staging.manual_rosetta_segmentation_global sg
        on (sg.name = 'Stress Test Segmentation (EBA)')
	 inner join MAX_DATE_GS mgs
        on (sg.partition_date = mgs.max_date);