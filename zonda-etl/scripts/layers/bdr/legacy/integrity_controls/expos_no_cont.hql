--El campo cuenta contable local debe estar informado	R0733326

--No deben existir registros duplicados	R0104326
select enc.e0627_feoperac as fecha_proceso,
    'R0104326' as cod_incidencia,
    count(*) as num_reg_total,
    count(*) - count(distinct E0627_CONTRA1 ) as num_reg_error,
    from_unixtime(unix_timestamp()) as op_timestamp
from bi_corp_bdr.jm_expos_no_con enc  
where enc.partition_date = '${partition_date}'
group by enc.e0627_feoperac;


--La Fecha datos se debe corresponder con la fecha de proceso de los datos	R0269326

select enc.e0627_feoperac as fecha_proceso,
    'R0269326' as cod_incidencia,
    count(*) as num_reg_total,
    sum(case when enc.e0627_feoperac = enc.E0627_FEC_MES then 0 else 1 end)  as num_reg_error,
    from_unixtime(unix_timestamp()) as op_timestamp
from bi_corp_bdr.jm_expos_no_con enc  
where enc.partition_date = '${partition_date}'
group by enc.e0627_feoperac;
--La Fecha mes se debe  corresponder con la fecha de proceso de datos	R0270326

select enc.e0627_feoperac as fecha_proceso,
    'R0270326' as cod_incidencia,
    count(*) as num_reg_total,
    sum(case when enc.e0627_feoperac = enc.E0627_FEC_MES then 0 else 1 end)  as num_reg_error,
    from_unixtime(unix_timestamp()) as op_timestamp
from bi_corp_bdr.jm_expos_no_con enc  
where enc.partition_date = '${partition_date}'
group by enc.e0627_feoperac;
--El valor del campo Empresa debe existir en la tabla de baremos correspondiente (3.2.6)	R1256326

select
    enc.e0627_feoperac as fecha_proceso,
    'R1256326' as cod_incidencia,
    count(*) as num_reg_total,
    sum(case when bgl.cod_baremo_global is null then 1 else 0 end) as num_reg_error,
    from_unixtime(unix_timestamp()) as op_timestamp
from bi_corp_bdr.jm_expos_no_con enc 
left join bi_corp_bdr.map_baremos_global_local bgl on
  bgl.cod_negocio = 72 and --Verificar el BAREMO
  bgl.cod_baremo_global = enc.E0627_S1EMP
where enc.partition_date = '${partition_date}'
group by enc.e0627_feoperac;

--El valor del campo Código de identificación entidad cargabal debe existir en la tabla de baremos correspondiente (3.2.6)	R1509326

select
    enc.e0627_feoperac as fecha_proceso,
    'R1509326' as cod_incidencia,
    count(*) as num_reg_total,
    sum(case when bgl.cod_baremo_global is null then 1 else 0 end) as num_reg_error,
    from_unixtime(unix_timestamp()) as op_timestamp
from bi_corp_bdr.jm_expos_no_con enc 
left join bi_corp_bdr.map_baremos_global_local bgl on
  bgl.cod_negocio = 72 and --Verificar el BAREMO
  bgl.cod_baremo_global = enc.e0627_ctacgbal
where enc.partition_date = '${partition_date}'
group by enc.e0627_feoperac;