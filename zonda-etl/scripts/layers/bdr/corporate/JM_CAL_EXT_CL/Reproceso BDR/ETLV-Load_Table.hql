set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=4000000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.exec.dynamic.partition.mode=nonstrict;
set mapred.job.queue.name=root.dataeng;

with perim_contr as (
            select mkg.nup as alias_nup, mkg.shortname
            from bi_corp_bdr.perim_mdr_contraparte mkg
            where  mkg.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201912_202103') }}'
)

insert overwrite table bi_corp_bdr.jm_cal_ext_cl
partition(partition_date= '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201912_202103') }}')
select R9415_FEOPERAC,
       R9415_S1EMP,
       R9415_IDNUMCLI,
       lpad(cast(nvl(R9415_COD_AGEN,0) as string),5,'0') as R9415_COD_AGEN,
       lpad(cast(nvl(R9415_CCODPLZ,0)  as string),5,'0') as R9415_CCODPLZ,
       lpad(cast(nvl(R9415_TIPMONED,0) as string),5,'0') as R9415_TIPMONED,
       to_date(from_unixtime(UNIX_TIMESTAMP(nvl(R9415_FECCALIF,'0001-01-01'),"yyyyMMdd"))),
       rpad(nvl(R9415_CALIFMAE,' '),40,' ') as R9415_CALIFMAE
from
(
	select 1 as orden,
	bi.g4093_feoperac as R9415_FEOPERAC, bi.g4093_s1emp as R9415_S1EMP, bi.g4093_idnumcli as R9415_IDNUMCLI,
	case when (rte.mdys_lt_fc is NULL or rte.mdys_lt_fc = 'WR') and rte.moodys_lt_rt is NULL then cast(null as string) else 2 end as R9415_COD_AGEN,
	case when (rte.mdys_lt_fc is NULL or rte.mdys_lt_fc = 'WR') and rte.moodys_lt_rt is NULL then cast(null as string) else 3 end as R9415_CCODPLZ,
	case when (rte.mdys_lt_fc is NULL or rte.mdys_lt_fc = 'WR') and rte.moodys_lt_rt is NULL then cast(null as string) else 2 end as R9415_TIPMONED,
	case when (rte.mdys_lt_fc is not NULL and rte.mdys_lt_fc <> 'WR') then rte.mdys_lt_fc_dt
		 when (rte.moodys_lt_rt is not NULL) then rte.moodys_lt_dt
		 else cast(null as string) end as R9415_FECCALIF,
	case when (rte.mdys_lt_fc is NULL or rte.mdys_lt_fc = 'WR') then rte.moodys_lt_rt else rte.mdys_lt_fc end as R9415_CALIFMAE
	from bi_corp_staging.aqua_rating_empresas rte  inner join perim_contr co
		on rte.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='max_partition_aqua_rating_empresas', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201912_202103') }}' and co.shortname = rte.unidad_operativa
		inner join bi_corp_bdr.jm_client_bii bi
	    on bi.g4093_idnumcli = lpad(co.alias_nup,9,'0') and bi.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201912_202103') }}'
	union all
	select 2 as orden,
	bi.g4093_feoperac as R9415_FEOPERAC, bi.g4093_s1emp as R9415_S1EMP, bi.g4093_idnumcli as R9415_IDNUMCLI,
	case when (rte.mdys_lt_lc is NULL or rte.mdys_lt_lc = 'WR') and rte.moodys_lt_rt is NULL then cast(null as string) else 2 end as R9415_COD_AGEN,
	case when (rte.mdys_lt_lc is NULL or rte.mdys_lt_lc = 'WR') and rte.moodys_lt_rt is NULL then cast(null as string) else 3 end as R9415_CCODPLZ,
	case when (rte.mdys_lt_lc is NULL or rte.mdys_lt_lc = 'WR') and rte.moodys_lt_rt is NULL then cast(null as string) else 1 end as R9415_TIPMONED,
	case when (rte.mdys_lt_lc is not NULL and rte.mdys_lt_lc <> 'WR') then rte.mdys_lt_lc_dt
		 when (rte.moodys_lt_rt is not NULL) then rte.moodys_lt_dt
		 else cast(null as string) end as R9415_FECCALIF,
	case when (rte.mdys_lt_lc is NULL or rte.mdys_lt_lc = 'WR') then moodys_lt_rt else rte.mdys_lt_lc end as R9415_CALIFMAE
	from bi_corp_staging.aqua_rating_empresas rte  inner join perim_contr co
		on rte.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='max_partition_aqua_rating_empresas', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201912_202103') }}' and co.shortname = rte.unidad_operativa
		inner join bi_corp_bdr.jm_client_bii bi
	    on bi.g4093_idnumcli = lpad(co.alias_nup,9,'0') and bi.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201912_202103') }}'
	union all
	select 3 as orden,
	bi.g4093_feoperac as R9415_FEOPERAC, bi.g4093_s1emp as R9415_S1EMP, bi.g4093_idnumcli as R9415_IDNUMCLI,
	CASE WHEN rte.sp_st_lcur_credit_rt is null THEN cast(null as string) else 1 end as R9415_COD_AGEN,
	CASE WHEN rte.sp_st_lcur_credit_rt is null THEN cast(null as string) else 1 end as R9415_CCODPLZ,
	CASE WHEN rte.sp_st_lcur_credit_rt is null THEN cast(null as string) else 1 end as R9415_TIPMONED,
	CASE WHEN rte.sp_st_lcur_credit_rt is null THEN cast(null as string) else rte.sp_st_lc_isr_cr_rt_dt end as R9415_FECCALIF,
	CASE WHEN rte.sp_st_lcur_credit_rt is null THEN cast(null as string) else rte.sp_st_lcur_credit_rt end as R9415_CALIFMAE
	from bi_corp_staging.aqua_rating_empresas rte  inner join perim_contr co
		on rte.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='max_partition_aqua_rating_empresas', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201912_202103') }}' and co.shortname = rte.unidad_operativa
		inner join bi_corp_bdr.jm_client_bii bi
	    on bi.g4093_idnumcli = lpad(co.alias_nup,9,'0') and bi.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201912_202103') }}'
	union all
	select 4 as orden,
	bi.g4093_feoperac as R9415_FEOPERAC, bi.g4093_s1emp as R9415_S1EMP, bi.g4093_idnumcli as R9415_IDNUMCLI,
	CASE WHEN rte.sp_st_fcur_credit_rt is null THEN cast(null as string) else 1 end as R9415_COD_AGEN,
	CASE WHEN rte.sp_st_fcur_credit_rt is null THEN cast(null as string) else 1 end as R9415_CCODPLZ,
	CASE WHEN rte.sp_st_fcur_credit_rt is null THEN cast(null as string) else 2 end as R9415_TIPMONED,
	CASE WHEN rte.sp_st_fcur_credit_rt is null THEN cast(null as string) else rte.sp_st_fc_isr_cr_rt_dt end as R9415_FECCALIF,
	CASE WHEN rte.sp_st_fcur_credit_rt is null THEN cast(null as string) else rte.sp_st_fcur_credit_rt end as R9415_CALIFMAE
	from bi_corp_staging.aqua_rating_empresas rte  inner join perim_contr co
		on rte.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='max_partition_aqua_rating_empresas', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201912_202103') }}' and co.shortname = rte.unidad_operativa
		inner join bi_corp_bdr.jm_client_bii bi
	    on bi.g4093_idnumcli = lpad(co.alias_nup,9,'0') and bi.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201912_202103') }}'
	union all
	select 5 as orden,
	bi.g4093_feoperac as R9415_FEOPERAC, bi.g4093_s1emp as R9415_S1EMP, bi.g4093_idnumcli as R9415_IDNUMCLI,
	CASE WHEN rte.sp_lt_fcur_credit_rt is null THEN cast(null as string) else 1 end as R9415_COD_AGEN,
	CASE WHEN rte.sp_lt_fcur_credit_rt is null THEN cast(null as string) else 3 end as R9415_CCODPLZ,
	CASE WHEN rte.sp_lt_fcur_credit_rt is null THEN cast(null as string) else 2 end as R9415_TIPMONED,
	CASE WHEN rte.sp_lt_fcur_credit_rt is null THEN cast(null as string) else rte.sp_lt_fc_isr_cr_rt_dt end as R9415_FECCALIF,
	CASE WHEN rte.sp_lt_fcur_credit_rt is null THEN cast(null as string) else rte.sp_lt_fcur_credit_rt end as R9415_CALIFMAE
	from bi_corp_staging.aqua_rating_empresas rte  inner join perim_contr co
		on rte.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='max_partition_aqua_rating_empresas', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201912_202103') }}' and co.shortname = rte.unidad_operativa
		inner join bi_corp_bdr.jm_client_bii bi
	    on bi.g4093_idnumcli = lpad(co.alias_nup,9,'0') and bi.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201912_202103') }}'
	union all
	select 6 as orden,
	bi.g4093_feoperac as R9415_FEOPERAC, bi.g4093_s1emp as R9415_S1EMP, bi.g4093_idnumcli as R9415_IDNUMCLI,
	CASE WHEN rte.sp_lt_lcur_credit_rt is null THEN cast(null as string) else 1 end as R9415_COD_AGEN,
	CASE WHEN rte.sp_lt_lcur_credit_rt is null THEN cast(null as string) else 3 end as R9415_CCODPLZ,
	CASE WHEN rte.sp_lt_lcur_credit_rt is null THEN cast(null as string) else 1 end as R9415_TIPMONED,
	CASE WHEN rte.sp_lt_lcur_credit_rt is null THEN cast(null as string) else rte.sp_lt_lc_isr_cr_rt_dt end as R9415_FECCALIF,
	CASE WHEN rte.sp_lt_lcur_credit_rt is null THEN cast(null as string) else rte.sp_lt_lcur_credit_rt end as R9415_CALIFMAE
	from bi_corp_staging.aqua_rating_empresas rte  inner join perim_contr co
		on rte.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='max_partition_aqua_rating_empresas', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201912_202103') }}' and co.shortname = rte.unidad_operativa
		inner join bi_corp_bdr.jm_client_bii bi
	    on bi.g4093_idnumcli = lpad(co.alias_nup,9,'0') and bi.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201912_202103') }}'
	union all
	select 7 as orden,
	bi.g4093_feoperac as R9415_FEOPERAC, bi.g4093_s1emp as R9415_S1EMP, bi.g4093_idnumcli as R9415_IDNUMCLI,
	case when rte.moodys_st_debt_rating is NULL then cast(null as string) else 2 end as R9415_COD_AGEN,
	case when rte.moodys_st_debt_rating is NULL then cast(null as string) else 1 end as R9415_CCODPLZ,
	case when rte.moodys_st_debt_rating is NULL then cast(null as string) else 1 end as R9415_TIPMONED,
	case when rte.moodys_st_debt_rating is NULL then cast(null as string) else rte.moodys_st_rating_date end as R9415_FECCALIF,
	case when rte.moodys_st_debt_rating is NULL then cast(null as string) else rte.moodys_st_debt_rating end as R9415_CALIFMAE
	from bi_corp_staging.aqua_rating_empresas rte  inner join perim_contr co
		on rte.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='max_partition_aqua_rating_empresas', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201912_202103') }}' and co.shortname = rte.unidad_operativa
		inner join bi_corp_bdr.jm_client_bii bi
	    on bi.g4093_idnumcli = lpad(co.alias_nup,9,'0') and bi.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201912_202103') }}'
	union all
	select t.* from (
		select 8 as orden,
		bi.g4093_feoperac as R9415_FEOPERAC, bi.g4093_s1emp as R9415_S1EMP, bi.g4093_idnumcli as R9415_IDNUMCLI,
		case when rte.moodys_st_debt_rating is NULL then cast(null as string) else 2 end as R9415_COD_AGEN,
		case when rte.moodys_st_debt_rating is NULL then cast(null as string) else 1 end as R9415_CCODPLZ,
		case when rte.moodys_st_debt_rating is NULL then cast(null as string) else 2 end as R9415_TIPMONED,
		case when rte.moodys_st_debt_rating is NULL then cast(null as string) else rte.moodys_st_rating_date end as R9415_FECCALIF,
		case when rte.moodys_st_debt_rating is NULL then cast(null as string) else rte.moodys_st_debt_rating end as R9415_CALIFMAE
		from bi_corp_staging.aqua_rating_empresas rte  inner join perim_contr co
			on rte.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='max_partition_aqua_rating_empresas', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201912_202103') }}' and co.shortname = rte.unidad_operativa
			inner join bi_corp_bdr.jm_client_bii bi
	    	on bi.g4093_idnumcli = lpad(co.alias_nup,9,'0') and bi.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201912_202103') }}'
	) t
	where t.R9415_COD_AGEN is not null
) tabla
where R9415_COD_AGEN is not null or
	  R9415_CCODPLZ  is not null or
	  R9415_TIPMONED is not null or
	  R9415_FECCALIF is not null or
	  R9415_CALIFMAE is not null
order by R9415_IDNUMCLI;