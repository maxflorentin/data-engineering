SET mapred.job.queue.name=root.dataeng;

insert overwrite table bi_corp_nuevo_cargabal.master_pension_plan_detail
select 'PPDE1','Defined contribution','Aportación definida' union all
select 'PPDE2','Defined benefit','Prestación definida' ;