insert overwrite table bi_corp_nuevo_cargabal.master_group_minority_interests
select 'CONS1','Parent Company','Empresa matriz' union all
select 'CONS2','Group Consolidation - Global method','Consolidación. Grupo - Global' union all
select 'CONS3','Group Consolidation - Proportional method','Consolidación. Grupo - Proporcional' union all
select 'CONS4','Group Consolidation - Equity method','Consolidación. Grupo - Puesta en equivalencia' union all
select 'CONS5','Consolidation - Minority interests','Consolidación. Minoritarios' ;