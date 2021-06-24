insert overwrite table bi_corp_nuevo_cargabal.master_acc_portfolio_reclassification
select 'APFR1','From Fair Value through other comprehensive income','Desde Valor razonable con cambios en otro resultado global' union all
select 'APFR2','From amortised cost','Desde coste amortizado' union all
select 'APFR3','Non reclassification','Sin recalificación' ;