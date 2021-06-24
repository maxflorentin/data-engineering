SET mapred.job.queue.name=root.dataeng;

insert overwrite table bi_corp_nuevo_cargabal.master_policyholder_risk
select 'ULINK1','Yes','Sí' union all
select 'ULINK2','No','No' ;

