insert into bi_corp_bdr.log_controles_auxiliar partition(tabla='bi_corp_bdr.jm_posic_contr')
select
    jmpc.partition_date as fecha_proceso,
    'R0024324321' as cod_incidencia,
    count(*) as num_reg_total,
    SUM(case when icm.num_cuenta is null then 1 else 0 end) as num_reg_error,
    current_timestamp() as op_timestamp
from bi_corp_bdr.jm_posic_contr jmpc
         left join bi_corp_bdr.id_cto_map icm on jmpc.e0621_contra1=icm.id_cto_bdr
where jmpc.partition_date = '${partition_date}'
group by jmpc.partition_date
;

insert into bi_corp_bdr.log_controles_auxiliar partition(tabla='bi_corp_bdr.jm_posic_contr')
select
    jmpc.partition_date as fecha_proceso,
    'R0279324' as cod_incidencia,
    count(*) as num_reg_total,
    SUM(case when length(trim(E0621_CTA_CONT))>0 then 0 else 1 end ) as num_reg_error,
    current_timestamp() as op_timestamp
from bi_corp_bdr.jm_posic_contr jmpc
where jmpc.partition_date = '${partition_date}'
group by jmpc.partition_date
;

insert into bi_corp_bdr.log_controles_auxiliar partition(tabla='bi_corp_bdr.jm_posic_contr')
select
    jmpc.partition_date as fecha_proceso,
    'R0102324' as cod_incidencia,
    count(*) as num_reg_total,
    count(*) - count(distinct jmpc.e0621_feoperac,jmpc.e0621_contra1,jmpc.e0621_cta_cont,jmpc.e0621_importh,jmpc.e0621_tip_impt) as num_reg_error,
    current_timestamp() as op_timestamp
from bi_corp_bdr.jm_posic_contr jmpc
where jmpc.partition_date = '${partition_date}'
group by jmpc.partition_date
;

insert into bi_corp_bdr.log_controles_auxiliar partition(tabla='bi_corp_bdr.jm_posic_contr')
select
    jmpc.partition_date as fecha_proceso,
    'R0276324' as cod_incidencia,
    count(*) as num_reg_total,
    SUM(case when substr(jmpc.e0621_feoperac, 1, 7) = jmpc.partition_date then 0 else 1 end) as num_reg_error,
    current_timestamp() as op_timestamp
from bi_corp_bdr.jm_posic_contr jmpc
where jmpc.partition_date = '${partition_date}'
group by jmpc.partition_date
;

insert into bi_corp_bdr.log_controles_auxiliar partition(tabla='bi_corp_bdr.jm_posic_contr')
select
    jmpc.partition_date as fecha_proceso,
    'R0282324' as cod_incidencia,
    count(*) as num_reg_total,
    SUM(case when length(trim(E0621_CODDIV))=0 then 1 else 0 end ) as num_reg_error,
    current_timestamp() as op_timestamp
from bi_corp_bdr.jm_posic_contr jmpc
where jmpc.partition_date = '${partition_date}'
group by jmpc.partition_date
;

insert into bi_corp_bdr.log_controles_auxiliar partition(tabla='bi_corp_bdr.jm_posic_contr')
select
    jmpc.partition_date as fecha_proceso,
    'R0827324' as cod_incidencia,
    count(*) as num_reg_total,
    SUM(case when jmpc.e0621_feoperac=jmpc.e0621_fec_mes then 0 else 1 end) as num_reg_error,
    current_timestamp() as op_timestamp
from bi_corp_bdr.jm_posic_contr jmpc
where jmpc.partition_date = '${partition_date}'
group by jmpc.partition_date
;

insert into bi_corp_bdr.log_controles_auxiliar partition(tabla='bi_corp_bdr.jm_posic_contr')
select
    jmpc.partition_date as fecha_proceso,
    'R0828324' as cod_incidencia,
    count(*) as num_reg_total,
    SUM(case when jmpc.e0621_fec_mes ='0001-01-01' then 1 else 0 end) as num_reg_error,
    current_timestamp() as op_timestamp
from bi_corp_bdr.jm_posic_contr jmpc
where jmpc.partition_date = '${partition_date}'
group by jmpc.partition_date
;

insert into bi_corp_bdr.log_controles_auxiliar partition(tabla='bi_corp_bdr.jm_posic_contr')
select
    jmpc.partition_date as fecha_proceso,
    'R0829324' as cod_incidencia,
    count(*) as num_reg_total,
    SUM(case when jmpc.e0621_fec_mes = '9999-12-31' then 1 else 0 end) as num_reg_error,
    current_timestamp() as op_timestamp
from bi_corp_bdr.jm_posic_contr jmpc
where jmpc.partition_date = '${partition_date}'
group by jmpc.partition_date
;

insert into bi_corp_bdr.log_controles_auxiliar partition(tabla='bi_corp_bdr.jm_posic_contr')
select
    jmpc.partition_date as fecha_proceso,
    'R1250324' as cod_incidencia,
    count(*) as num_reg_total,
    SUM(case when bl.cod_negocio is null then 1 else 0 end ) as num_reg_error,
    current_timestamp() as op_timestamp
from bi_corp_bdr.jm_posic_contr jmpc
         left join bi_corp_bdr.v_map_baremos_global_local bl
                   on  bl.cod_negocio = 1
                       and bl.cod_baremo_global=jmpc.e0621_s1emp
where jmpc.partition_date = '${partition_date}'
group by jmpc.partition_date
;

insert into bi_corp_bdr.log_controles_auxiliar partition(tabla='bi_corp_bdr.jm_posic_contr')
select
    jmpc.partition_date as fecha_proceso,
    'R1251324' as cod_incidencia,
    count(*) as num_reg_total,
    SUM(case when bl.cod_negocio is null then 1 else 0 end ) as num_reg_error,
    current_timestamp() as op_timestamp
from bi_corp_bdr.jm_posic_contr jmpc
         left join bi_corp_bdr.v_map_baremos_global_local bl
                   on  bl.cod_negocio = 6
                       and bl.cod_baremo_global=E0621_CODDIV
where jmpc.partition_date = '${partition_date}'
group by jmpc.partition_date
;
