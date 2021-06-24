SET mapred.job.queue.name=root.dataeng;

insert overwrite table bi_corp_nuevo_cargabal.master_performing_status
select 'PFST1','Performing exposures','Performing status' union all
select 'PFST2','Non-performing exposures','Non performing status' ;