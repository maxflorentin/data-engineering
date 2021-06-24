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

-- inserto nkey capital
INSERT OVERWRITE TABLE bi_corp_common.rosetta_nkey
PARTITION (domain_code)
select distinct
       le.code legal_entity_code,
	   ctr.g4095_contra1 native_key,
       case when trim(nor.cred_cod_entidad)= ''
	   	then nor.id_cto_source
			else concat(nor.cred_cod_entidad,
		    			nor.cred_cod_centro,
		                nor.cred_num_cuenta,
		                nor.cred_cod_producto,
		                nor.cred_cod_subprodu_altair
		                )
        end master_key,
        '9999-12-31' end_date,
		'{last_calendar_day}' partition_date,
		 gd.domain_code
from bi_corp_bdr.jm_contr_bis ctr
	 left join bi_corp_bdr.normalizacion_id_contratos nor
	 	on (ctr.g4095_contra1 = nor.id_cto_bdr
	 	    and ctr.partition_date = nor.partition_date)
	 inner join bi_corp_staging.manual_rosetta_global_domains gd
	 	on(gd.second_name = 'Capital')
	 inner join bi_corp_staging.manual_rosetta_legal_entity_unit_global le
	 	on (le.domain = 'LEGAL ENTITY')
	 inner join MAX_DATE_GD mgd
	 	on(gd.partition_date = mgd.max_date)
	 inner join MAX_DATE_LE mle
	 	on(le.partition_date = mle.max_date)
where ctr.partition_date = from_unixtime(unix_timestamp('{last_calendar_day}','yyyy-MM-dd'),'yyyy-MM');