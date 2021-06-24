SET mapred.job.queue.name=root.dataeng;

insert overwrite table bi_corp_nuevo_cargabal.master_spanish_corporate_income_tax
select 'SPAN1','Yes','Sí' union all
select 'SPAN2','No','No' ;