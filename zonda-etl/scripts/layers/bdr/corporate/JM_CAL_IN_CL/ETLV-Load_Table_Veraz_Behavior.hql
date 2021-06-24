set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=4000000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.exec.dynamic.partition.mode=nonstrict;
set mapred.job.queue.name=root.dataeng;

--VERAZ BEHAVIOR
with perim_veraz as (
	select distinct concat('0',nup) as numcli, riesgo_score, fec_carga 
	from bi_corp_staging.veraz_behavior 
	where partition_date = '{{ var.json.jm_cal_in_cl.particion_veraz_behavior}}' 
		and riesgo_score != '' 
		and cast(riesgo_score as int) > 0
		and	nup != ''
),
perim_clientes_bis as (
	select g4093_idnumcli as numcli, g4093_tipsegl2 as tipsegl2
	from bi_corp_bdr.jm_client_bii 
	where partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_Tables-Monthly') }}'
		and trim(g4093_tipsegl2) in ('C1','A','B','C','S')
),
join_perimetros as (
	select v.numcli, v.riesgo_score, v.fec_carga, trim(cb.tipsegl2) as tipsegl2
	from perim_veraz v left join perim_clientes_bis cb
		on v.numcli = cb.numcli
	where cb.tipsegl2 is not null 
)
insert overwrite table bi_corp_bdr.jm_cal_in_cl 
partition(partition_date= '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_Tables-Monthly') }}')
select '{{ ti.xcom_pull(task_ids='InputConfig', key='last_calendar_day', dag_id='BDR_LOAD_Tables-Monthly') }}' as e9954_feoperac, 
	   '23100' as e9954_s1emp,    
	   numcli as e9954_idnumcli, 
	   from_unixtime(unix_timestamp(fec_carga ,'dd/MM/yyyy'), 'yyyy-MM-dd') as e9954_feccali,  
	   case when tipsegl2 in ('A','B','C','S') then '00026'
	   		when tipsegl2 in ('C1') then '00036' 
	   		else null end as e9954_tipmodel, 
	   case when tipsegl2 in ('A','B','C','S') then '00327'
	   		when tipsegl2 in ('C1') then '00328' 
	   		else null end as e9954_tipmode2,
	   case when tipsegl2 in ('A','B','C','S') then '000000327'
	   		when tipsegl2 in ('C1') then '000000328' 
	   		else null end as e9954_idmodel,  
	   null as e9954_tipo,
	   lpad(concat(riesgo_score,'0000000'),11,'0') as e9954_idpunsco,
	   to_date(cast(add_months(from_unixtime(unix_timestamp(fec_carga ,'dd/MM/yyyy'), 'yyyy-MM-dd'),6) as string)) as e9954_feccaduc,
       '00000' as e9954_c1tarpun, 
       '00000' as e9954_c1spid,
       '00000' as e9954_c1digcon, 
       '{{ ti.xcom_pull(task_ids='InputConfig', key='last_calendar_day', dag_id='BDR_LOAD_Tables-Monthly') }}' as e0621_fecultmo, 
       '00000' as e9954_motivfor,
       '00000000000' as e9954_idpunsc2, 
       '9999-12-31' as e9954_fecrepfi, 
       '0001-01-01' as e9954_fecinofc
from join_perimetros
union all
select e9954_feoperac,e9954_s1emp,   e9954_idnumcli,e9954_feccali, e9954_tipmodel,e9954_tipmode2,
       e9954_idmodel, e9954_tipo,    e9954_idpunsco,e9954_feccaduc,e9954_c1tarpun,e9954_c1spid,
       e9954_c1digcon,e0621_fecultmo,e9954_motivfor,e9954_idpunsc2,e9954_fecrepfi,e9954_fecinofc  
from bi_corp_bdr.jm_cal_in_cl
where partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_Tables-Monthly') }}';