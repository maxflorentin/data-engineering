insert into bi_corp_bdr.log_controles_auxiliar partition(tabla='bi_corp_bdr.jm_contr_otros')
select
    otros.partition_date as fecha_proceso,
    'R0025325321' as cod_incidencia,
    count(*) as num_reg_total,
    sum(case when bis.g4095_contra1 is null then 1 else 0 end) as num_reg_error,
    current_timestamp() as op_timestamp
from bi_corp_bdr.jm_contr_otros otros
         left join bi_corp_bdr.jm_contr_bis bis
                   on  bis.g4095_contra1 = otros.e0623_contra1 and
                       bis.partition_date = otros.partition_date
where otros.partition_date = '${partition_date}'
group by otros.partition_date
;
insert into bi_corp_bdr.log_controles_auxiliar partition(tabla='bi_corp_bdr.jm_contr_otros')
select
    otros.partition_date as fecha_proceso,
    'R0103325' as cod_incidencia,
    count(*) as num_reg_total,
    count(*) - count(distinct otros.e0623_contra1) as num_reg_error,
    current_timestamp() as op_timestamp
from bi_corp_bdr.jm_contr_otros otros
where otros.partition_date = '${partition_date}'
group by otros.partition_date
;
-- simple
insert into bi_corp_bdr.log_controles_auxiliar partition(tabla='bi_corp_bdr.jm_contr_otros')
select
    otros.partition_date as fecha_proceso,
    'R0250325' as cod_incidencia,
    count(*) as num_reg_total,
    sum(case when otros.e0623_ataemax between 0 and 100 then 0 else 1 end) as num_reg_error,
    current_timestamp() as op_timestamp
from bi_corp_bdr.jm_contr_otros otros
where otros.partition_date = '${partition_date}'
group by otros.partition_date
;
insert into bi_corp_bdr.log_controles_auxiliar partition(tabla='bi_corp_bdr.jm_contr_otros')
select
    otros.partition_date as fecha_proceso,
    'R0354325' as cod_incidencia,
    count(*) as num_reg_total,
    sum(case when otros.e0623_tip_int between 0 and 100 then 0 else 1 end) as num_reg_error,
    current_timestamp() as op_timestamp
from bi_corp_bdr.jm_contr_otros otros
where otros.partition_date = '${partition_date}'
group by otros.partition_date
;
insert into bi_corp_bdr.log_controles_auxiliar partition(tabla='bi_corp_bdr.jm_contr_otros')
select
    otros.partition_date as fecha_proceso,
    'R0355325' as cod_incidencia,
    count(*) as num_reg_total,
    sum(case when substr(otros.e0623_feoperac, 1, 7) = otros.partition_date then 0 else 1 end) as num_reg_error,
    current_timestamp() as op_timestamp
from bi_corp_bdr.jm_contr_otros otros
where otros.partition_date = '${partition_date}'
group by otros.partition_date
;
insert into bi_corp_bdr.log_controles_auxiliar partition(tabla='bi_corp_bdr.jm_contr_otros')
select
    otros.partition_date as fecha_proceso,
    'R0504325' as cod_incidencia,
    count(*) as num_reg_total,
    SUM(case when baremo.cod_negocio IS NULL then 1 else 0 end) as num_reg_error,
    current_timestamp() as op_timestamp
from bi_corp_bdr.jm_contr_otros otros
         left join bi_corp_bdr.v_map_baremos_global_local baremo
                   on  baremo.cod_negocio = 16 and
                       baremo.cod_baremo_global = otros.e0623_gest_sit
where otros.partition_date = '${partition_date}'
group by otros.partition_date
;
insert into bi_corp_bdr.log_controles_auxiliar partition(tabla='bi_corp_bdr.jm_contr_otros')
select
    otros.partition_date as fecha_proceso,
    'R0822325' as cod_incidencia,
    count(*) as num_reg_total,
    sum(case when substr(otros.e0623_fec_mes, 1, 7) = otros.partition_date then 0 else 1 end) as num_reg_error,
    current_timestamp() as op_timestamp
from bi_corp_bdr.jm_contr_otros otros
where otros.partition_date = '${partition_date}'
group by otros.partition_date
;
insert into bi_corp_bdr.log_controles_auxiliar partition(tabla='bi_corp_bdr.jm_contr_otros')
select
    otros.partition_date as fecha_proceso,
    'R0824325' as cod_incidencia,
    count(*) as num_reg_total,
    sum(case when otros.e0623_fec_mes = '0001-01-01' then 1 else 0 end) as num_reg_error,
    current_timestamp() as op_timestamp
from bi_corp_bdr.jm_contr_otros otros
where otros.partition_date = '${partition_date}'
group by otros.partition_date
;
insert into bi_corp_bdr.log_controles_auxiliar partition(tabla='bi_corp_bdr.jm_contr_otros')
select
    otros.partition_date as fecha_proceso,
    'R0825325' as cod_incidencia,
    count(*) as num_reg_total,
    sum(case when otros.e0623_fec_mes = '9999-12-31' then 1 else 0 end) as num_reg_error,
    current_timestamp() as op_timestamp
from bi_corp_bdr.jm_contr_otros otros
where otros.partition_date = '${partition_date}'
group by otros.partition_date
;
insert into bi_corp_bdr.log_controles_auxiliar partition(tabla='bi_corp_bdr.jm_contr_otros')
select
    otros.partition_date as fecha_proceso,
    'R1066325' as cod_incidencia,
    count(*) as num_reg_total,
    SUM(case when baremo.cod_negocio IS NULL then 1 else 0 end) as num_reg_error,
    current_timestamp() as op_timestamp
from bi_corp_bdr.jm_contr_otros otros
         left join bi_corp_bdr.v_map_baremos_global_local baremo
                   on  baremo.cod_negocio = 70 and
                       baremo.cod_baremo_global = otros.e0623_estadtrj
where otros.partition_date = '${partition_date}'
group by otros.partition_date
;
insert into bi_corp_bdr.log_controles_auxiliar partition(tabla='bi_corp_bdr.jm_contr_otros')
select
    otros.partition_date as fecha_proceso,
    'R1067325' as cod_incidencia,
    count(*) as num_reg_total,
    SUM(case when baremo.cod_negocio IS NULL then 1 else 0 end) as num_reg_error,
    current_timestamp() as op_timestamp
from bi_corp_bdr.jm_contr_otros otros
         left join bi_corp_bdr.v_map_baremos_global_local baremo
                   on  baremo.cod_negocio = 72 and
                       baremo.cod_baremo_global = otros.e0623_unnt
where otros.partition_date = '${partition_date}'
group by otros.partition_date
;
insert into bi_corp_bdr.log_controles_auxiliar partition(tabla='bi_corp_bdr.jm_contr_otros')
select
    otros.partition_date as fecha_proceso,
    'R1068325' as cod_incidencia,
    count(*) as num_reg_total,
    SUM(case when baremo.cod_negocio IS NULL then 1 else 0 end) as num_reg_error,
    current_timestamp() as op_timestamp
from bi_corp_bdr.jm_contr_otros otros
         left join bi_corp_bdr.v_map_baremos_global_local baremo
                   on  baremo.cod_negocio = 73 and
                       baremo.cod_baremo_global = otros.e0623_unnts
where otros.partition_date = '${partition_date}'
group by otros.partition_date
;
insert into bi_corp_bdr.log_controles_auxiliar partition(tabla='bi_corp_bdr.jm_contr_otros')
select
    otros.partition_date as fecha_proceso,
    'R1071325' as cod_incidencia,
    count(*) as num_reg_total,
    sum(case when otros.e0623_rgosub not in ('S','N') then 1 else 0 end) as num_reg_error,
    current_timestamp() as op_timestamp
from bi_corp_bdr.jm_contr_otros otros
where otros.partition_date = '${partition_date}'
group by otros.partition_date
;
insert into bi_corp_bdr.log_controles_auxiliar partition(tabla='bi_corp_bdr.jm_contr_otros')
select
    otros.partition_date as fecha_proceso,
    'R1074325' as cod_incidencia,
    count(*) as num_reg_total,
    sum(case when otros.e0623_intneg not in ('S','N') then 1 else 0 end) as num_reg_error,
    current_timestamp() as op_timestamp
from bi_corp_bdr.jm_contr_otros otros
where otros.partition_date = '${partition_date}'
group by otros.partition_date
;
insert into bi_corp_bdr.log_controles_auxiliar partition(tabla='bi_corp_bdr.jm_contr_otros')
select
    otros.partition_date as fecha_proceso,
    'R1075325' as cod_incidencia,
    count(*) as num_reg_total,
    SUM(case when baremo.cod_negocio IS NULL then 1 else 0 end) as num_reg_error,
    current_timestamp() as op_timestamp
from bi_corp_bdr.jm_contr_otros otros
         left join bi_corp_bdr.v_map_baremos_global_local baremo
                   on  baremo.cod_negocio = 78 and
                       baremo.cod_baremo_global = otros.e0623_mtvalta
where otros.partition_date = '${partition_date}'
group by otros.partition_date
;
insert into bi_corp_bdr.log_controles_auxiliar partition(tabla='bi_corp_bdr.jm_contr_otros')
select
    otros.partition_date as fecha_proceso,
    'R1076325' as cod_incidencia,
    count(*) as num_reg_total,
    sum(case when otros.e0623_indsubro not in ('S','N') then 1 else 0 end) as num_reg_error,
    current_timestamp() as op_timestamp
from bi_corp_bdr.jm_contr_otros otros
where otros.partition_date = '${partition_date}'
group by otros.partition_date
;
insert into bi_corp_bdr.log_controles_auxiliar partition(tabla='bi_corp_bdr.jm_contr_otros')
select
    otros.partition_date as fecha_proceso,
    'R1077325' as cod_incidencia,
    count(*) as num_reg_total,
    SUM(case when baremo.cod_negocio IS NULL then 1 else 0 end) as num_reg_error,
    current_timestamp() as op_timestamp
from bi_corp_bdr.jm_contr_otros otros
         left join bi_corp_bdr.v_map_baremos_global_local baremo
                   on  baremo.cod_negocio = 8 and
                       baremo.cod_baremo_global = otros.e0623_tip_inte
where otros.partition_date = '${partition_date}'
group by otros.partition_date
;
insert into bi_corp_bdr.log_controles_auxiliar partition(tabla='bi_corp_bdr.jm_contr_otros')
select
    otros.partition_date as fecha_proceso,
    'R1078325' as cod_incidencia,
    count(*) as num_reg_total,
    sum(case when otros.e0623_indsegur not in ('S','N') then 1 else 0 end) as num_reg_error,
    current_timestamp() as op_timestamp
from bi_corp_bdr.jm_contr_otros otros
where otros.partition_date = '${partition_date}'
group by otros.partition_date
;
insert into bi_corp_bdr.log_controles_auxiliar partition(tabla='bi_corp_bdr.jm_contr_otros')
select
    otros.partition_date as fecha_proceso,
    'R1079325' as cod_incidencia,
    count(*) as num_reg_total,
    sum(case when otros.e0623_amortpar not in ('S','N') then 1 else 0 end) as num_reg_error,
    current_timestamp() as op_timestamp
from bi_corp_bdr.jm_contr_otros otros
where otros.partition_date = '${partition_date}'
group by otros.partition_date
;
insert into bi_corp_bdr.log_controles_auxiliar partition(tabla='bi_corp_bdr.jm_contr_otros')
select
    otros.partition_date as fecha_proceso,
    'R1082325' as cod_incidencia,
    count(*) as num_reg_total,
    SUM(case when baremo.cod_negocio IS NULL then 1 else 0 end) as num_reg_error,
    current_timestamp() as op_timestamp
from bi_corp_bdr.jm_contr_otros otros
         left join bi_corp_bdr.v_map_baremos_global_local baremo
                   on  baremo.cod_negocio = 80 and
                       baremo.cod_baremo_global = otros.e0623_estprinm
where otros.partition_date = '${partition_date}'
group by otros.partition_date
;
insert into bi_corp_bdr.log_controles_auxiliar partition(tabla='bi_corp_bdr.jm_contr_otros')
select
    otros.partition_date as fecha_proceso,
    'R1252325' as cod_incidencia,
    count(*) as num_reg_total,
    SUM(case when baremo.cod_negocio IS NULL then 1 else 0 end) as num_reg_error,
    current_timestamp() as op_timestamp
from bi_corp_bdr.jm_contr_otros otros
         left join bi_corp_bdr.v_map_baremos_global_local baremo
                   on  baremo.cod_negocio = 1 and
                       baremo.cod_baremo_global = otros.e0623_emprepor
where otros.partition_date = '${partition_date}'
group by otros.partition_date
;
insert into bi_corp_bdr.log_controles_auxiliar partition(tabla='bi_corp_bdr.jm_contr_otros')
select
    otros.partition_date as fecha_proceso,
    'R1253325' as cod_incidencia,
    count(*) as num_reg_total,
    SUM(case when baremo.cod_negocio IS NULL then 1 else 0 end) as num_reg_error,
    current_timestamp() as op_timestamp
from bi_corp_bdr.jm_contr_otros otros
         left join bi_corp_bdr.v_map_baremos_global_local baremo
                   on  baremo.cod_negocio = 13 and
                       baremo.cod_baremo_global = otros.e0623_tip_liqu
where otros.partition_date = '${partition_date}'
group by otros.partition_date
;
insert into bi_corp_bdr.log_controles_auxiliar partition(tabla='bi_corp_bdr.jm_contr_otros')
select
    otros.partition_date as fecha_proceso,
    'R1254325' as cod_incidencia,
    count(*) as num_reg_total,
    SUM(case when baremo.cod_negocio IS NULL then 1 else 0 end) as num_reg_error,
    current_timestamp() as op_timestamp
from bi_corp_bdr.jm_contr_otros otros
         left join bi_corp_bdr.v_map_baremos_global_local baremo
                   on  baremo.cod_negocio = 521 and
                       baremo.cod_baremo_global = otros.e0623_tip_amor
where otros.partition_date = '${partition_date}'
group by otros.partition_date
;
insert into bi_corp_bdr.log_controles_auxiliar partition(tabla='bi_corp_bdr.jm_contr_otros')
select
    otros.partition_date as fecha_proceso,
    'R1255325' as cod_incidencia,
    count(*) as num_reg_total,
    SUM(case when baremo.cod_negocio IS NULL then 1 else 0 end) as num_reg_error,
    current_timestamp() as op_timestamp
from bi_corp_bdr.jm_contr_otros otros
         left join bi_corp_bdr.v_map_baremos_global_local baremo
                   on  baremo.cod_negocio = 17 and
                       baremo.cod_baremo_global = otros.e0623_inv_nego
where otros.partition_date = '${partition_date}'
group by otros.partition_date
;
insert into bi_corp_bdr.log_controles_auxiliar partition(tabla='bi_corp_bdr.jm_contr_otros')
select
    otros.partition_date as fecha_proceso,
    'R1387325' as cod_incidencia,
    count(*) as num_reg_total,
    SUM(case when baremo.cod_negocio IS NULL then 1 else 0 end) as num_reg_error,
    current_timestamp() as op_timestamp
from bi_corp_bdr.jm_contr_otros otros
         left join bi_corp_bdr.v_map_baremos_global_local baremo
                   on  baremo.cod_negocio = 83 and
                       baremo.cod_baremo_global = otros.e0623_codimphi
where otros.partition_date = '${partition_date}'
group by otros.partition_date
;
insert into bi_corp_bdr.log_controles_auxiliar partition(tabla='bi_corp_bdr.jm_contr_otros')
select
    otros.partition_date as fecha_proceso,
    'R1445325' as cod_incidencia,
    count(*) as num_reg_total,
    SUM(case when baremo.cod_negocio IS NULL then 1 else 0 end) as num_reg_error,
    current_timestamp() as op_timestamp
from bi_corp_bdr.jm_contr_otros otros
         left join bi_corp_bdr.v_map_baremos_global_local baremo
                   on  baremo.cod_negocio = 91 and
                       baremo.cod_baremo_global = otros.e0623_tipcaren
where otros.partition_date = '${partition_date}'
group by otros.partition_date
;
insert into bi_corp_bdr.log_controles_auxiliar partition(tabla='bi_corp_bdr.jm_contr_otros')
select
    otros.partition_date as fecha_proceso,
    'R1446325' as cod_incidencia,
    count(*) as num_reg_total,
    sum(case when otros.e0623_ibuysell not in ('B','S') then 1 else 0 end) as num_reg_error,
    current_timestamp() as op_timestamp
from bi_corp_bdr.jm_contr_otros otros
where otros.partition_date = '${partition_date}'
group by otros.partition_date
;
insert into bi_corp_bdr.log_controles_auxiliar partition(tabla='bi_corp_bdr.jm_contr_otros')
select
    otros.partition_date as fecha_proceso,
    'R1447325' as cod_incidencia,
    count(*) as num_reg_total,
    sum(case when otros.e0623_sutipint between 0 and 100 then 0 else 1 end) as num_reg_error,
    current_timestamp() as op_timestamp
from bi_corp_bdr.jm_contr_otros otros
where otros.partition_date = '${partition_date}'
group by otros.partition_date
;
insert into bi_corp_bdr.log_controles_auxiliar partition(tabla='bi_corp_bdr.jm_contr_otros')
select
    otros.partition_date as fecha_proceso,
    'R1448325' as cod_incidencia,
    count(*) as num_reg_total,
    sum(case when otros.e0623_tetipint between 0 and 100 then 0 else 1 end) as num_reg_error,
    current_timestamp() as op_timestamp
from bi_corp_bdr.jm_contr_otros otros
where otros.partition_date = '${partition_date}'
group by otros.partition_date
;
insert into bi_corp_bdr.log_controles_auxiliar partition(tabla='bi_corp_bdr.jm_contr_otros')
select
    jco.partition_date as fecha_proceso,
    'R1449325' as cod_incidencia,
    count(*) as num_reg_total,
    sum(case when bgl.cod_baremo_global is null then 1 else 0 end) as num_reg_error,
    current_timestamp() as op_timestamp
from bi_corp_bdr.jm_contr_otros jco
         left join bi_corp_bdr.v_map_baremos_global_local bgl on
            bgl.cod_negocio = 13 and
            bgl.cod_baremo_global = jco.e0623_tipsuelo
where jco.partition_date = '${partition_date}'
group by jco.partition_date
;
insert into bi_corp_bdr.log_controles_auxiliar partition(tabla='bi_corp_bdr.jm_contr_otros')
select
    jco.partition_date as fecha_proceso,
    'R1450325' as cod_incidencia,
    count(*) as num_reg_total,
    sum(case when bgl.cod_baremo_global is null then 1 else 0 end) as num_reg_error,
    current_timestamp() as op_timestamp
from bi_corp_bdr.jm_contr_otros jco
         left join bi_corp_bdr.v_map_baremos_global_local bgl on
            bgl.cod_negocio = 13 and
            bgl.cod_baremo_global = jco.e0623_tiptecho
where jco.partition_date = '${partition_date}'
group by jco.partition_date
;
insert into bi_corp_bdr.log_controles_auxiliar partition(tabla='bi_corp_bdr.jm_contr_otros')
select
    jco.partition_date as fecha_proceso,
    'R1451325' as cod_incidencia,
    count(*) as num_reg_total,
    sum(case when bgl.cod_baremo_global is null then 1 else 0 end) as num_reg_error,
    current_timestamp() as op_timestamp
from bi_corp_bdr.jm_contr_otros jco
         left join bi_corp_bdr.v_map_baremos_global_local bgl on
            bgl.cod_negocio = 47 and
            bgl.cod_baremo_global = jco.e0623_plrevtin
where jco.partition_date = '${partition_date}'
group by jco.partition_date
;
insert into bi_corp_bdr.log_controles_auxiliar partition(tabla='bi_corp_bdr.jm_contr_otros')
select
    jco.partition_date as fecha_proceso,
    'R1452325' as cod_incidencia,
    count(*) as num_reg_total,
    sum(case when (jco.e0623_feccuota < jco.e0623_feoperac) then 1 else 0 end) as num_reg_error,
    current_timestamp() as op_timestamp
from bi_corp_bdr.jm_contr_otros jco
where jco.partition_date = '${partition_date}'
group by jco.partition_date
;
insert into bi_corp_bdr.log_controles_auxiliar partition(tabla='bi_corp_bdr.jm_contr_otros')
select
    jco.partition_date as fecha_proceso,
    'R1453325' as cod_incidencia,
    count(*) as num_reg_total,
    sum(case when (jco.e0623_nudiaatr < 0) then 1 else 0 end) as num_reg_error,
    current_timestamp() as op_timestamp
from bi_corp_bdr.jm_contr_otros jco
where jco.partition_date = '${partition_date}'
group by jco.partition_date
;
insert into bi_corp_bdr.log_controles_auxiliar partition(tabla='bi_corp_bdr.jm_contr_otros')
select
    jco.partition_date as fecha_proceso,
    'R1454325' as cod_incidencia,
    count(*) as num_reg_total,
    sum(case when jco.e0623_marcauti not in ("S", "N", " ") then 1 else 0 end) as num_reg_error,
    current_timestamp() as op_timestamp
from bi_corp_bdr.jm_contr_otros jco
where jco.partition_date = '${partition_date}'
group by jco.partition_date
;
insert into bi_corp_bdr.log_controles_auxiliar partition(tabla='bi_corp_bdr.jm_contr_otros')
select
    jco.partition_date as fecha_proceso,
    'R1506325' as cod_incidencia,
    count(*) as num_reg_total,
    sum(case when (jco.e0623_tipinefe < 0 or jco.e0623_tipinefe > 100) then 1 else 0 end) as num_reg_error,
    current_timestamp() as op_timestamp
from bi_corp_bdr.jm_contr_otros jco
where jco.partition_date = '${partition_date}'
group by jco.partition_date
;
insert into bi_corp_bdr.log_controles_auxiliar partition(tabla='bi_corp_bdr.jm_contr_otros')
select
    jco.partition_date as fecha_proceso,
    'R1507325' as cod_incidencia,
    count(*) as num_reg_total,
    sum(case when bgl.cod_baremo_global is null then 1 else 0 end) as num_reg_error,
    current_timestamp() as op_timestamp
from bi_corp_bdr.jm_contr_otros jco
         left join bi_corp_bdr.v_map_baremos_global_local bgl on
            bgl.cod_negocio = 1 and
            bgl.cod_baremo_global = jco.e0623_s1emp
where jco.partition_date = '${partition_date}'
group by jco.partition_date
;
insert into bi_corp_bdr.log_controles_auxiliar partition(tabla='bi_corp_bdr.jm_contr_otros')
select
    jco.partition_date as fecha_proceso,
    'R1508325' as cod_incidencia,
    count(*) as num_reg_total,
    sum(case when jco.e0623_s1emp = 0 then 1 else 0 end) as num_reg_error,
    current_timestamp() as op_timestamp
from bi_corp_bdr.jm_contr_otros jco
where jco.partition_date = '${partition_date}'
group by jco.partition_date
;
insert into bi_corp_bdr.log_controles_auxiliar partition(tabla='bi_corp_bdr.jm_contr_otros')
select
    jco.partition_date as fecha_proceso,
    'R1544325' as cod_incidencia,
    count(*) as num_reg_total,
    sum(case when jco.e0623_forpgact is null then 1 else 0 end) as num_reg_error,
    current_timestamp() as op_timestamp
from bi_corp_bdr.jm_contr_otros jco
where jco.partition_date = '${partition_date}'
group by jco.partition_date
