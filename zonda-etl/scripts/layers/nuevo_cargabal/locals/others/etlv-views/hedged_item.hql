SET mapred.job.queue.name=root.dataeng;

insert overwrite table bi_corp_nuevo_cargabal.master_hedged_item
select 'HEDI1','Assets','Activo' union all
select 'HEDI2','Liabilities','Pasivo' ;
