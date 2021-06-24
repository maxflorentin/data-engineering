INSERT INTO bi_corp_bdr.log_controles
--El código identificador de la garantía debe existir en la tabla Garantía Real
select jgt.partition_date as fecha_proceso,
        'R0053353352' as cod_incidencia,
        'bi_corp_bdr.jm_val_gara' as tabla,
        count(*) as num_reg_total,
        SUM(case when lcb.g4091_biengar1 IS NULL then 1 else 0 end) as num_reg_error,
        current_timestamp() as op_timestamp
from bi_corp_bdr.jm_val_gara jgt left join bi_corp_bdr.jm_gara_real lcb on lcb.g4091_biengar1 = jgt.g4134_biengar1
AND jgt.partition_date = lcb.partition_date
where jgt.partition_date = ${month_partition_bdr}
group by jgt.partition_date
UNION ALL
--La fecha de valoración debe ser menor o igual que la fecha en la que se está procesando la información
select jgt.partition_date as fecha_proceso,
        'R0054353' as cod_incidencia,
        'bi_corp_bdr.jm_val_gara' as tabla,
        count(*) as num_reg_total,
        SUM(case when jgt.g4134_fechvalr > current_date() then 1 else 0 end) as num_reg_error,
        current_timestamp() as op_timestamp
from bi_corp_bdr.jm_val_gara jgt
where jgt.partition_date = ${month_partition_bdr}
group by jgt.partition_date
UNION ALL
--El campo importe de la garantía debe estar informado
select jgt.partition_date as fecha_proceso,
        'R0305353' as cod_incidencia,
        'bi_corp_bdr.jm_val_gara' as tabla,
        count(*) as num_reg_total,
        SUM(case when jgt.g4134_imgarant IS NULL OR trim(jgt.g4134_imgarant) = '' then 1 else 0 end) as num_reg_error,
        current_timestamp() as op_timestamp
from bi_corp_bdr.jm_val_gara jgt
where jgt.partition_date = ${month_partition_bdr}
group by jgt.partition_date
UNION ALL
--No deben existir registros duplicados
select jgt.partition_date as fecha_proceso,
        'R0113353' as cod_incidencia,
        'bi_corp_bdr.jm_val_gara' as tabla,
        count(*) as num_reg_total,
        count(*) - count(distinct g4134_feoperac, g4134_s1emp, g4134_biengar1, g4134_fechvalr, g4134_nomenti, g4134_imgarant, g4134_fecultmo, g4134_tipvaln, g4134_tip_gara, g4134_coddiv, partition_date) as num_reg_error,
        current_timestamp() as op_timestamp
from bi_corp_bdr.jm_val_gara jgt
where jgt.partition_date = ${month_partition_bdr}
group by jgt.partition_date
UNION ALL
--La Fecha de valoración debe ser distinta de ‘0001-01-01
select jgt.partition_date as fecha_proceso,
        'R0841353' as cod_incidencia,
        'bi_corp_bdr.jm_val_gara' as tabla,
        count(*) as num_reg_total,
        SUM(case when jgt.g4134_fechvalr = '0001-01-01' then 1 else 0 end) as num_reg_error,
        current_timestamp() as op_timestamp
from bi_corp_bdr.jm_val_gara jgt
where jgt.partition_date = ${month_partition_bdr}
group by jgt.partition_date
UNION ALL
--La Fecha de valoración debe ser distinta de ‘9999-12-31’
select jgt.partition_date as fecha_proceso,
        'R0842353' as cod_incidencia,
        'bi_corp_bdr.jm_val_gara' as tabla,
        count(*) as num_reg_total,
        SUM(case when jgt.g4134_fechvalr = '9999-12-31' then 1 else 0 end) as num_reg_error,
        current_timestamp() as op_timestamp
from bi_corp_bdr.jm_val_gara jgt
where jgt.partition_date = ${month_partition_bdr}
group by jgt.partition_date
UNION ALL
--El valor del campo Empresa debe existir en la tabla de baremos correspondiente (3.5.3)
select jgt.partition_date as fecha_proceso,
        'R1345353' as cod_incidencia,
        'bi_corp_bdr.jm_val_gara' as tabla,
        count(*) as num_reg_total,
        SUM(case when bgl.cod_baremo_global IS NULL then 1 else 0 end) as num_reg_error,
        current_timestamp() as op_timestamp
from bi_corp_bdr.jm_val_gara jgt
left join bi_corp_bdr.v_map_baremos_global_local bgl on jgt.g4134_s1emp = bgl.cod_baremo_global
AND bgl.cod_negocio_local = 1
where jgt.partition_date = ${month_partition_bdr}
group by jgt.partition_date
UNION ALL
--El valor del campo Tipo de Valoración debe existir en la tabla de baremos correspondiente (3.5.3)
select jgt.partition_date as fecha_proceso,
        'R1346353' as cod_incidencia,
        'bi_corp_bdr.jm_val_gara' as tabla,
        count(*) as num_reg_total,
        SUM(case when bgl.cod_baremo_global IS NULL then 1 else 0 end) as num_reg_error,
        current_timestamp() as op_timestamp
from bi_corp_bdr.jm_val_gara jgt
left join bi_corp_bdr.v_map_baremos_global_local bgl on jgt.g4134_tipvaln = bgl.cod_baremo_global
AND bgl.cod_negocio_local = 109
where jgt.partition_date = ${month_partition_bdr}
group by jgt.partition_date
UNION ALL
--El valor del campo Tipo de Garantía debe existir en la tabla de baremos correspondiente (3.5.3)
select jgt.partition_date as fecha_proceso,
        'R1391353' as cod_incidencia,
        'bi_corp_bdr.jm_val_gara' as tabla,
        count(*) as num_reg_total,
        SUM(case when bgl.cod_baremo_global IS NULL then 1 else 0 end) as num_reg_error,
        current_timestamp() as op_timestamp
from bi_corp_bdr.jm_val_gara jgt
left join bi_corp_bdr.v_map_baremos_global_local bgl on jgt.g4134_tip_gara = bgl.cod_baremo_global
AND bgl.cod_negocio_local = 25
where jgt.partition_date = ${month_partition_bdr}
group by jgt.partition_date
UNION ALL
--El id de la garantía debe existir en la tabla Garantías Contrato  (3.5.3)
select jgt.partition_date as fecha_proceso,
        'R1392355' as cod_incidencia,
        'bi_corp_bdr.jm_val_gara' as tabla,
        count(*) as num_reg_total,
        SUM(case when gcto.g4124_biengar1 IS NULL then 1 else 0 end) as num_reg_error,
        current_timestamp() as op_timestamp
from bi_corp_bdr.jm_val_gara jgt
left join bi_corp_bdr.jm_garant_cto gcto on gcto.g4124_biengar1 = jgt.g4134_biengar1
AND jgt.partition_date = gcto.partition_date
where jgt.partition_date = ${month_partition_bdr}
group by jgt.partition_date;
