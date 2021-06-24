SET mapred.job.queue.name=root.dataeng;

insert overwrite table bi_corp_nuevo_cargabal.master_tangible_asset_purpose
select 'TAPU1','Own Use','Uso propio' union all
select 'TAPU2','Leased out under an operating lease','Arrendado bajo un arrendamiento operativo' union all
select 'TAPU3','Without use','Sin uso' ;