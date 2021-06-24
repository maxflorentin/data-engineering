insert into bi_corp_bdr.log_controles_auxiliar partition(tabla='bi_corp_bdr.jm_interv_cto')

select
    jmic.partition_date as fecha_proceso,
    'R0014322321' as cod_incidencia,
    count(*) as num_reg_total,
    sum(case when icm.id_cto_bdr is null then 1 else 0 end) as num_reg_error,
    current_timestamp() as op_timestamp
from bi_corp_bdr.JM_INTERV_CTO jmic
left join bi_corp_bdr.id_cto_map icm
       on jmic.g4128_contra1=icm.id_cto_bdr
where jmic.partition_date = '${partition_date}'
group by jmic.partition_date

; insert into bi_corp_bdr.log_controles_auxiliar partition(tabla='bi_corp_bdr.jm_interv_cto')

select
    jmic.partition_date as fecha_proceso,
    'R0017322341' as cod_incidencia,
    count(*) as num_reg_total,
    sum(case when cli.g4093_idnumcli is null then 1 else 0 end) as num_reg_error,
    current_timestamp() as op_timestamp
from bi_corp_bdr.JM_INTERV_CTO jmic
left join bi_corp_bdr.jm_client_bii cli
       on jmic.g4128_idnumcli=cli.g4093_idnumcli
where jmic.partition_date = '${partition_date}'
group by jmic.partition_date

;
 insert into bi_corp_bdr.log_controles_auxiliar partition(tabla='bi_corp_bdr.jm_interv_cto')

select
    jmic.partition_date as fecha_proceso,
    'R0018322' as cod_incidencia,
    count(*) as num_reg_total,
    sum(case when (G4128_FORMINTV!='00004' and cast(G4128_PORPARTN as int) != 100) then 1 else 0 end) as num_reg_error,
    current_timestamp() as op_timestamp
from bi_corp_bdr.JM_INTERV_CTO jmic
where jmic.partition_date = '${partition_date}'
group by jmic.partition_date

;
 insert into bi_corp_bdr.log_controles_auxiliar partition(tabla='bi_corp_bdr.jm_interv_cto')

select
    jmic.partition_date as fecha_proceso,
    'R0111322' as cod_incidencia,
    count(*) as num_reg_total,
    count(*) - count(distinct jmic.g4128_feoperac,jmic.g4128_idnumcli,jmic.g4128_contra1,jmic.g4128_tipintev,jmic.g4128_tipintv2,jmic.g4128_formintv,jmic.g4128_porpartn) as num_reg_error,
    current_timestamp() as op_timestamp
from bi_corp_bdr.JM_INTERV_CTO jmic
where jmic.partition_date = '${partition_date}'
group by jmic.partition_date

;
 insert into bi_corp_bdr.log_controles_auxiliar partition(tabla='bi_corp_bdr.jm_interv_cto')

select
    jmic.partition_date as fecha_proceso,
    'R0502322' as cod_incidencia,
    count(*) as num_reg_total,
    sum(case when bl.cod_negocio is null then 1 else 0 end) as num_reg_error,
    current_timestamp() as op_timestamp
from bi_corp_bdr.JM_INTERV_CTO jmic
         left join bi_corp_bdr.v_map_baremos_global_local bl
                   on  bl.cod_negocio = 10 and
                       bl.cod_baremo_global = jmic.g4128_tipintev
where jmic.partition_date = '${partition_date}'
group by jmic.partition_date

;
 insert into bi_corp_bdr.log_controles_auxiliar partition(tabla='bi_corp_bdr.jm_interv_cto')

select
    jmic.partition_date as fecha_proceso,
    'R1268322' as cod_incidencia,
    count(*) as num_reg_total,
    sum(case when bl.cod_negocio is null then 1 else 0 end) as num_reg_error,
    current_timestamp() as op_timestamp
from bi_corp_bdr.JM_INTERV_CTO jmic
         left join bi_corp_bdr.v_map_baremos_global_local bl
                   on  bl.cod_negocio = 1
                       and bl.cod_baremo_global=jmic.g4128_s1emp
where jmic.partition_date = '${partition_date}'
group by jmic.partition_date

;
 insert into bi_corp_bdr.log_controles_auxiliar partition(tabla='bi_corp_bdr.jm_interv_cto')

select
    jmic.partition_date as fecha_proceso,
    'R1269322' as cod_incidencia,
    count(*) as num_reg_total,
    sum(case when bl.cod_negocio is null then 1 else 0 end) as num_reg_error,
    current_timestamp() as op_timestamp
from bi_corp_bdr.JM_INTERV_CTO jmic
         left join bi_corp_bdr.v_map_baremos_global_local bl
                   on  bl.cod_negocio = 11 and
                       bl.cod_baremo_global = jmic.G4128_FORMINTV
where jmic.partition_date = '${partition_date}'
group by jmic.partition_date

;
