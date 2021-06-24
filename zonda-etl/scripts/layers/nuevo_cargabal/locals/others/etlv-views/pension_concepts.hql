SET mapred.job.queue.name=root.dataeng;

insert overwrite table bi_corp_nuevo_cargabal.master_pension_concepts
select 'PENC1','Defined contribution','Aportación definida' union all
select 'PENC2','Other long term defined benefit','Otras retribuciones a largo plazo' union all
select 'PENC3','Payment commitments to early retirees','Payment commitments to early retirees' union all
select 'PENC4','Termination benefits','Indemnizaciones por despido' union all
select 'PENC5','External RD 1588/99','External RD 1588/99' union all
select 'PENC6','Internal RD 1588/99','Internal RD 1588/99' union all
select 'PENC7','Other post employment','Otras prestaciones post-empleo' union all
select 'PENC8','Vested','Causada' union all
select 'PENC9','Other','Otros' union all
select 'PENC10','Unvested accrued','No causada devengada' union all
select 'PENC11','Unvested not accrued','No causada no devengada' ;