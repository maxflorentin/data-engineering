insert overwrite table bi_corp_nuevo_cargabal.master_assets_origin
select 'ORIG1','Purchase','Compra' union all
select 'ORIG2','Finance lease','Arrendamiento financiero' union all
select 'ORIG3','Operating lease','Arrendamiento operativo' union all
select 'ORIG4','Foreclosed/Dation in payment','Adjudicado Dacion en pago' ;