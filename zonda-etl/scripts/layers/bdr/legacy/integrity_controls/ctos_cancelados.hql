insert into bi_corp_bdr.log_controles_auxiliar partition(tabla='bi_corp_bdr.jm_ctos_cance')
--No deben existir registros duplicados 	R11993212
select
    core.partition_date as fecha_proceso,
    "R11993212" as cod_incidencia,
    count(*) as num_reg_total,
    (count(*) - count(distinct H0711_FEOPERAC, H0711_S1EMP, H0711_CONTRA1, H0711_MOTVCANC, H0711_FECULTMO)) as num_reg_error,
    current_timestamp() as op_timestamp
from bi_corp_bdr.jm_ctos_cance core
where core.partition_date = '${partition_date}'
group by core.partition_date
;

insert into bi_corp_bdr.log_controles_auxiliar partition(tabla='bi_corp_bdr.jm_ctos_cance')
--El valor del campo Empresa debe existir en la tabla de baremos correspondiente (3.2.12)	R12723212
select
    core.partition_date as fecha_proceso,
    "R12723212" as cod_incidencia,
    count(*) as num_reg_total,
    SUM(case when baremo.cod_negocio IS NULL then 1 else 0 end) as num_reg_error,
    current_timestamp() as op_timestamp
from bi_corp_bdr.jm_ctos_cance core
         left join bi_corp_bdr.v_map_baremos_global_local baremo
                   on  baremo.cod_negocio = 1 and
                       baremo.cod_baremo_global = core.h0711_s1emp
where core.partition_date = '${partition_date}'
group by core.partition_date;

insert into bi_corp_bdr.log_controles_auxiliar partition(tabla='bi_corp_bdr.jm_ctos_cance')
--El identificador de contrato debe existir en la tabla Contratos (3.2.12)	R10903212
select
    core.partition_date as fecha_proceso,
    "R10903212" as cod_incidencia,
    count(*) as num_reg_total,
    sum(case when core.h0711_contra1 is null then 1 else 0 end) as num_reg_error,
    current_timestamp() as op_timestamp
from bi_corp_bdr.jm_ctos_cance core 
        left join bi_corp_bdr.jm_contr_bis cbi 
            on core.h0711_contra1 = cbi.g4095_contra1 
            and cbi.partition_date = '${partition_date}' 
where core.partition_date = '${partition_date}'
group by core.partition_date;