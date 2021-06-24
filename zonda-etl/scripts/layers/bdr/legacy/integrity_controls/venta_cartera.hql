--No deben existir registros duplicados (3.14.2)	R15793142

SELECT vcr.r9736_feoperac as fecha_proceso,
    'R15793142' as cod_incidencia,
    count(*) as num_reg_total,
    count(*) - count(distinct vcr.r9736_contra1 ) as num_reg_error,
    from_unixtime(unix_timestamp()) as op_timestamp
from bi_corp_bdr.jm_vta_carter vcr 
where vcr.partition_date = '${partition_date}'
group by vcr.r9736_feoperac;

--El valor del campo Empresa no existe (3.14.2)	R15803142

SELECT vcr.r9736_feoperac as fecha_proceso,
    'R15803142' as cod_incidencia,
    count(*) as num_reg_total,
    sum(case when vcr.r9736_s1emp is null then 1 else 0) as num_reg_error,
    from_unixtime(unix_timestamp()) as op_timestamp
from bi_corp_bdr.jm_vta_carter vcr 
where vcr.partition_date = '${partition_date}'
group by vcr.r9736_feoperac;




--El identificador del contrato debe estar informado y no ser cero (3.14.2)	R15813142

SELECT vcr.r9736_feoperac as fecha_proceso,
    'R15803142' as cod_incidencia,
    count(*) as num_reg_total,
    sum(case when vcr.r9736_contra1 = 000000000 then 1 else 0) as num_reg_error,
    from_unixtime(unix_timestamp()) as op_timestamp
from bi_corp_bdr.jm_vta_carter vcr 
where vcr.partition_date = '${partition_date}'
group by vcr.r9736_feoperac;


--El valor del campo indicador Credit Related no es válida (3.14.2)	R15823142

SELECT vcr.r9736_feoperac as fecha_proceso,
    'R15823142' as cod_incidencia,
    count(*) as num_reg_total,
    sum(case when vcr.r9736_ind_credit <> 'N' then 1 else 0) as num_reg_error, 
    from_unixtime(unix_timestamp()) as op_timestamp
from bi_corp_bdr.jm_vta_carter vcr 
where vcr.partition_date = '${partition_date}'
group by vcr.r9736_feoperac;
