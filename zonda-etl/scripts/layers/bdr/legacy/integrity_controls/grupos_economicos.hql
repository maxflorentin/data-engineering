--No deben existir registros duplicados	R0126343
SELECT gec.g5512_feoperac as fecha_proceso,
    'R0126343' as cod_incidencia,
    count(*) as num_reg_total,
    count(*) - count(gec.g5512_grup_eco) as num_reg_error,
    from_unixtime(unix_timestamp()) as op_timestamp
from bi_corp_bdr.jm_grup_econo gec 
where gec.partition_date = '${partition_date}'
group by gec.g5512_feoperac;


--El valor del campo Sector Grupo Económico debe existir en la tabla de baremos correspondiente 	R1097343

select
    gec.g5512_feoperac as fecha_proceso,
    'R1097343' as cod_incidencia,
    count(*) as num_reg_total,
    sum(case when bgl.cod_baremo_global is null then 1 else 0 end) as num_reg_error,
    from_unixtime(unix_timestamp()) as op_timestamp
from bi_corp_bdr.jm_grup_econo gec 
left join bi_corp_bdr.map_baremos_global_local bgl on
  bgl.cod_negocio = 72 and --Verificar el BAREMO
  bgl.cod_baremo_global = gec.g5512_grecosec
where gec.partition_date = '${partition_date}'
group by gec.g5512_feoperac;


--El valor del campo Empresa debe existir en la tabla de baremos correspondiente (3.4.3)	R1298343

select
    gec.g5512_feoperac as fecha_proceso,
    'R1298343' as cod_incidencia,
    count(*) as num_reg_total,
    sum(case when bgl.cod_baremo_global is null then 1 else 0 end) as num_reg_error,
    from_unixtime(unix_timestamp()) as op_timestamp
from bi_corp_bdr.jm_grup_econo gec 
left join bi_corp_bdr.map_baremos_global_local bgl on
  bgl.cod_negocio = 1 and --Verificar el BAREMO
  bgl.cod_baremo_global = gec.g5512_s1emp
where gec.partition_date = '${partition_date}'
group by gec.g5512_feoperac;

--El valor del campo País debe existir en la tabla de baremos correspondiente (3.4.3)	R1299343

select
    gec.g5512_feoperac as fecha_proceso,
    'R1299343' as cod_incidencia,
    count(*) as num_reg_total,
    sum(case when bgl.cod_baremo_global is null then 1 else 0 end) as num_reg_error,
    from_unixtime(unix_timestamp()) as op_timestamp
from bi_corp_bdr.jm_grup_econo gec 
left join bi_corp_bdr.map_baremos_global_local bgl on
  bgl.cod_negocio = 1 and --Verificar el BAREMO
  bgl.cod_baremo_global = gec.g5512_id_pais
where gec.partition_date = '${partition_date}'
group by gec.g5512_feoperac;

--El valor del campo Origen Facturación debe existir en la tabla de baremos correspondiente (3.4.3)	R1300343

select
    gec.g5512_feoperac as fecha_proceso,
    'R1300343' as cod_incidencia,
    count(*) as num_reg_total,
    sum(case when bgl.cod_baremo_global is null then 1 else 0 end) as num_reg_error,
    from_unixtime(unix_timestamp()) as op_timestamp
from bi_corp_bdr.jm_grup_econo gec 
left join bi_corp_bdr.map_baremos_global_local bgl on
  bgl.cod_negocio = 1 and --Verificar el BAREMO
  bgl.cod_baremo_global = gec.g5512_orig_fac
where gec.partition_date = '${partition_date}'
group by gec.g5512_feoperac;


--El valor del campo Origen Activos debe existir en la tabla de baremos correspondiente (3.4.3)	R1301343

select
    gec.g5512_feoperac as fecha_proceso,
    'R1301343' as cod_incidencia,
    count(*) as num_reg_total,
    sum(case when bgl.cod_baremo_global is null then 1 else 0 end) as num_reg_error,
    from_unixtime(unix_timestamp()) as op_timestamp
from bi_corp_bdr.jm_grup_econo gec 
left join bi_corp_bdr.map_baremos_global_local bgl on
  bgl.cod_negocio = 1 and --Verificar el BAREMO
  bgl.cod_baremo_global = gec.g5512_orig_act
where gec.partition_date = '${partition_date}'
group by gec.g5512_feoperac;



--El valor del campo Origen Número de Empleados debe existir en la tabla de baremos correspondiente (3.4.3)	R1302343

select
    gec.g5512_feoperac as fecha_proceso,
    'R1302343' as cod_incidencia,
    count(*) as num_reg_total,
    sum(case when bgl.cod_baremo_global is null then 1 else 0 end) as num_reg_error,
    from_unixtime(unix_timestamp()) as op_timestamp
from bi_corp_bdr.jm_grup_econo gec 
left join bi_corp_bdr.map_baremos_global_local bgl on
  bgl.cod_negocio = 1 and --Verificar el BAREMO
  bgl.cod_baremo_global = gec.g5512_orig_emp
where gec.partition_date = '${partition_date}'
group by gec.g5512_feoperac;


--El valor del campo Divisa debe existir en la tabla de baremos correspondiente(3.4.3)	R1303343

select
    gec.g5512_feoperac as fecha_proceso,
    'R1303343' as cod_incidencia,
    count(*) as num_reg_total,
    sum(case when bgl.cod_baremo_global is null then 1 else 0 end) as num_reg_error,
    from_unixtime(unix_timestamp()) as op_timestamp
from bi_corp_bdr.jm_grup_econo gec 
left join bi_corp_bdr.map_baremos_global_local bgl on
  bgl.cod_negocio = 1 and --Verificar el BAREMO
  bgl.cod_baremo_global = gec.g5512_coddiv
where gec.partition_date = '${partition_date}'
group by gec.g5512_feoperac;



--El Total deuda grupo debe ser mayor o igual que cero (3.4.3)	R1459343

 
select
    gec.g5512_feoperac as fecha_proceso,
    'R1303343' as cod_incidencia,
    count(*) as num_reg_total,
    sum(case when g5512_tdeudagr >=0 then 0 else 1 end) as num_reg_error,
    from_unixtime(unix_timestamp()) as op_timestamp
from bi_corp_bdr.jm_grup_econo gec 
where gec.partition_date = '${partition_date}'
group by gec.g5512_feoperac;



--El valor del campo Marca empresa no obligada a tener facturación no es válido.	R15463412
--Revisar este control cuando la data este disponible 

select
    gec.g5512_feoperac as fecha_proceso,
    'R15463412' as cod_incidencia,
    count(*) as num_reg_total,
    sum(case when g5512_tdeudagr >=0 then 0 else 1 end) as num_reg_error,
    from_unixtime(unix_timestamp()) as op_timestamp
from bi_corp_bdr.jm_grup_econo gec 
where gec.partition_date = '${partition_date}'
group by gec.g5512_feoperac;
