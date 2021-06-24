insert overwrite table bi_corp_nuevo_cargabal.master_main_category
select 'ACPF1','Cash and cash balances at central banks and other demand deposits','Efectivo y saldos en efectivo en bancos centrales y otros depósitos a la vista' union all
select 'ACPF2','Held for trading','Activos financieros mantenidos para negociar' union all
select 'ACPF3','Non-trading financial assets mandatorily at fair value through profit or loss','Activos financieros no destinados a negociación valorados obligatoriamente a valor razonable con cambios en resultados' union all
select 'ACPF4','Financial instrument designated at fair value through profit or loss','Activos financieros designados a valor razonable con cambios en resultados' union all
select 'ACPF5','Financial assets at fair value through other comprehensive income','Activos financieros a valor razonable con cambios en otro resultado global' union all
select 'ACPF6','Amortised cost','Activos financieros a coste amortizado' union all
select 'ACPF7','Hedge accounting','Cobertura' ;