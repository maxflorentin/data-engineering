insert into bi_corp_bdr.log_controles partition(tabla='bi_corp_bdr.jm_prov_esotr_div')
--El valor del campo Empresa debe existir en la tabla de baremos correspondiente (3.1.2)
select
    jmp.n0625_feoperac as fecha_proceso,
    'R1411312' as cod_incidencia,
    'bi_corp_bdr.jm_prov_esotr_div' as tabla,
    count(*) as num_reg_total,
    SUM(case when jmp.n0625_s1emp is null then 1 else 0 end) as num_reg_error,
    from_unixtime(unix_timestamp()) as op_timestamp
from bi_corp_bdr.jm_prov_esotr_div jmp
where jmp.n0625_feoperac = '${partition_date}'
group by jmp.n0625_feoperac

insert into bi_corp_bdr.log_controles partition(tabla='bi_corp_bdr.jm_prov_esotr_div')
--El identificador de contrato debe existir en la tabla Contratos  (3.1.2)
select
    jmp.n0625_feoperac as fecha_proceso,
    'R1412312' as cod_incidencia,
    'bi_corp_bdr.jm_prov_esotr_div' as tabla,
    count(*) as num_reg_total,
    SUM(case when mapp.id_cto_bdr is null then 1 else 0 end) as num_reg_error,
    from_unixtime(unix_timestamp()) as op_timestamp
from bi_corp_bdr.jm_prov_esotr_div jmp
right join bi_corp_bdr.id_cto_map mapp
on jmp.n0625_contra1 = mapp.id_cto_bdr
where jmp.n0625_feoperac = '${partition_date}'
group by jmp.n0625_feoperac

insert into bi_corp_bdr.log_controles partition(tabla='bi_corp_bdr.jm_prov_esotr_div')
--No deben existir registros duplicados (3.1.2)
select
    jmp.n0625_feoperac as fecha_proceso,
    'R1415312' as cod_incidencia,
    'bi_corp_bdr.jm_prov_esotr_div' as tabla,
    count(*) as num_reg_total,
    count(*) - count(distinct concat(jmp.n0625_feoperac,jmp.n0625_contra1)) as num_reg_error,
    from_unixtime(unix_timestamp()) as op_timestamp
from bi_corp_bdr.jm_prov_esotr_div jmp
where jmp.n0625_feoperac = '${partition_date}'
group by jmp.n0625_feoperac

insert into bi_corp_bdr.log_controles partition(tabla='bi_corp_bdr.jm_prov_esotr_div')
--El valor del campo Tipo de importe2 debe existir en la tabla de baremos correspondiente (3.1.2)
select
    jmp.n0625_feoperac as fecha_proceso,
    'R1413312' as cod_incidencia,
    'bi_corp_bdr.jm_prov_esotr_div' as tabla,
    count(*) as num_reg_total,
    sum(case when bgl.cod_baremo_global is null then 1 else 0 end) as num_reg_error,
    from_unixtime(unix_timestamp()) as op_timestamp
from bi_corp_bdr.jm_prov_esotr_div jmp
left join bi_corp_bdr.v_map_baremos_global_local bgl on
  bgl.cod_negocio_local = 89 and
  bgl.cod_baremo_local = cast(jmp.n0625_tip_impt as int)
where jmp.n0625_feoperac = '${partition_date}'
group by jmp.n0625_feoperac

insert into bi_corp_bdr.log_controles partition(tabla='bi_corp_bdr.jm_prov_esotr_div')
--El valor del campo Divisa debe existir en la tabla de baremos correspondiente(3.1.2)
select
    jmp.n0625_feoperac as fecha_proceso,
    'R1414312' as cod_incidencia,
    'bi_corp_bdr.jm_prov_esotr_div' as tabla,
    count(*) as num_reg_total,
    sum(case when bgl.cod_baremo_global is null then 1 else 0 end) as num_reg_error,
    from_unixtime(unix_timestamp()) as op_timestamp
from bi_corp_bdr.jm_prov_esotr_div jmp
left join bi_corp_bdr.v_baremos_global  bgl on
  bgl.id = 6 and
  bgl.cod_baremo_global = jmp.n0625_coddiv
where jmp.n0625_feoperac = '${partition_date}'
group by jmp.n0625_feoperac

insert into bi_corp_bdr.log_controles partition(tabla='bi_corp_bdr.jm_prov_esotr_div')
--El valor del campo Stage no existe (3.1.2)
select
    jmp.n0625_feoperac as fecha_proceso,
    'R1562312' as cod_incidencia,
    'bi_corp_bdr.jm_prov_esotr_div' as tabla,
    count(*) as num_reg_total,
    sum(case when jmp.n0625_stage is null then 1 else 0 end) as num_reg_error,
    from_unixtime(unix_timestamp()) as op_timestamp
from bi_corp_bdr.jm_prov_esotr_div jmp
where jmp.n0625_feoperac = '${partition_date}'
group by jmp.n0625_feoperac
;