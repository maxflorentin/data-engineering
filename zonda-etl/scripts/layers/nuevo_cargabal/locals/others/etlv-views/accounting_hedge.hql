insert overwrite table bi_corp_nuevo_cargabal.master_accounting_hedge
select 'HEDG1','Cash flow hedge. Macro Hedge','Cobertura de flujo de efectivo. Macro cobertura' union all
select 'HEDG2','Cash flow hedge. Micro Hedge','Cobertura de flujo de efectivo. Micro cobertura' union all
select 'HEDG3','Fair value hedge. Macro hedge','Cobertura de valor razonable. Macro cobertura' union all
select 'HEDG4','Fair value hedge. Micro hedge','Cobertura de valor razonable. Micro cobertura' union all
select 'HEDG5','Hedge of net investments in a foreign operation [effective portion]','Cobertura de inversiones netas en un negocio en el extranjero' ;
