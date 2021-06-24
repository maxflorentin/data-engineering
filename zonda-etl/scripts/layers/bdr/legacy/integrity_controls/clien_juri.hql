SELECT cjr.g5508_feoperac as fecha_proceso,
    'R0030342341' as cod_incidencia,
    count(*) as num_reg_total,
    sum(case when cjr.g5508_idnumcli is null then 1 else 0 end ) as num_reg_error,
    from_unixtime(unix_timestamp()) as op_timestamp
from bi_corp_bdr.jm_clien_juri cjr 
    left join  bi_corp_bdr.jm_client_bii cbi on cjr.g5508_idnumcli = cbi.g4093_idnumcli
where cjr.partition_date = '${partition_date}'
group by cjr.g5508_feoperac ;

--No deben existir registros duplicados	R0123342
SELECT cjr.g5508_feoperac as fecha_proceso,
    'R0030342341' as cod_incidencia,
    count(*) as num_reg_total,
    count(*) - count(cjr.g5508_idnumcli) as num_reg_error,
    from_unixtime(unix_timestamp()) as op_timestamp
from bi_corp_bdr.jm_clien_juri cjr 
where cjr.partition_date = '${partition_date}'
group by cjr.g5508_feoperac ;

--Fecha información debe ser distinta de ‘0001-01-01’	R0777342
SELECT cjr.g5508_feoperac as fecha_proceso,
    'R0777342' as cod_incidencia,
    count(*) as num_reg_total,
     sum(case when cjr.g5508_inffecha = '0001-01-01' then 1 else 0 end ) as num_reg_error,
    from_unixtime(unix_timestamp()) as op_timestamp
from bi_corp_bdr.jm_clien_juri cjr 
where cjr.partition_date = '${partition_date}'
group by cjr.g5508_feoperac ;

--Fecha información debe ser distinta de ‘9999-12-31’	R0778342
SELECT cjr.g5508_feoperac as fecha_proceso,
    'R0778342' as cod_incidencia,
    count(*) as num_reg_total,
     sum(case when cjr.g5508_inffecha = '9999-12-31' then 1 else 0 end ) as num_reg_error,
    from_unixtime(unix_timestamp()) as op_timestamp
from bi_corp_bdr.jm_clien_juri cjr 
where cjr.partition_date = '${partition_date}'
group by cjr.g5508_feoperac ;

--El valor del campo Empresa debe existir en la tabla de baremos correspondiente (3.4.2)	R1294342
SELECT cjr.g5508_feoperac as fecha_proceso,
    'R1294342' as cod_incidencia,
    count(*) as num_reg_total,
    sum(case when bgl.cod_baremo_global is null then 1 else 0 end) as num_reg_error,
    from_unixtime(unix_timestamp()) as op_timestamp
from bi_corp_bdr.jm_clien_juri cjr 
left join bi_corp_bdr.map_baremos_global_local bgl
on bgl.cod_negocio = 1
   and bgl.cod_baremo_global = cjr.g5508_s1emp
where cjr.partition_date = '${partition_date}'
group by cjr.g5508_feoperac ;

--El valor del campo Origen Facturación debe existir en la tabla de baremos correspondiente (3.4.2)	R1295342
SELECT cjr.g5508_feoperac as fecha_proceso,
    'R1295342' as cod_incidencia,
    count(*) as num_reg_total,
    sum(case when bgl.cod_baremo_global is null then 1 else 0 end) as num_reg_error,
    from_unixtime(unix_timestamp()) as op_timestamp
from bi_corp_bdr.jm_clien_juri cjr 
left join bi_corp_bdr.map_baremos_global_local bgl on
  bgl.cod_negocio = 1 and --Verificar el BAREMO GAP LOCAL
  bgl.cod_baremo_global = cjr.g5508_orig_fac
where cjr.partition_date = '${partition_date}'
group by cjr.g5508_feoperac ;

--El valor del campo Origen Activos debe existir en la tabla de baremos correspondiente (3.4.2)	R1296342
SELECT cjr.g5508_feoperac as fecha_proceso,
    'R1296342' as cod_incidencia,
    count(*) as num_reg_total,
    sum(case when bgl.cod_baremo_global is null then 1 else 0 end) as num_reg_error,
    from_unixtime(unix_timestamp()) as op_timestamp
from bi_corp_bdr.jm_clien_juri cjr 
left join bi_corp_bdr.map_baremos_global_local bgl on
  bgl.cod_negocio = 1 and --Verificar el BAREMO GAP LOCAL
  bgl.cod_baremo_global = cjr.g5508_orig_act
where cjr.partition_date = '${partition_date}'
group by cjr.g5508_feoperac ;

--El valor del campo Código identificación cargabal debe existir en la tabla de baremos correspondiente (3.4.2)	R1297342
SELECT cjr.g5508_feoperac as fecha_proceso,
    'R1297342' as cod_incidencia,
    count(*) as num_reg_total,
    sum(case when bgl.cod_baremo_global is null then 1 else 0 end) as num_reg_error,
    from_unixtime(unix_timestamp()) as op_timestamp
from bi_corp_bdr.jm_clien_juri cjr 
left join bi_corp_bdr.map_baremos_global_local bgl on
  bgl.cod_negocio = 1 and --Verificar el BAREMO GAP LOCAL
  bgl.cod_baremo_global = cjr.g5508_orig_emp
where cjr.partition_date = '${partition_date}'
group by cjr.g5508_feoperac ;

--El Total deuda cliente debe ser mayor o igual que cero (3.4.2)	R1458342
SELECT cjr.g5508_feoperac as fecha_proceso,
    'R1458342' as cod_incidencia,
    count(*) as num_reg_total,
     sum(case when cjr.g5508_tdeudacl >= 0 then 0 else 1 end ) as num_reg_error,
    from_unixtime(unix_timestamp()) as op_timestamp
from bi_corp_bdr.jm_clien_juri cjr
where cjr.partition_date = '${partition_date}'
group by cjr.g5508_feoperac ;

--El valor del Ratio CET1 debe estar entre 0 y 100 (3.4.11)	R15103411
SELECT cjr.g5508_feoperac as fecha_proceso,
    'R15103411' as cod_incidencia,
    count(*) as num_reg_total,
    sum(case when cjr.g5508_rat_cet1 >= 0 and cjr.g5508_rat_cet1 <= 100 then 0 else 1 end ) as num_reg_error,
    from_unixtime(unix_timestamp()) as op_timestamp
from bi_corp_bdr.jm_clien_juri cjr
where cjr.partition_date = '${partition_date}'
group by cjr.g5508_feoperac ;

--El valor de la Tasa de Mora debe estar entre 0 y 100 (3.4.11)	R15113411
SELECT cjr.g5508_feoperac as fecha_proceso,
    'R15113411' as cod_incidencia,
    count(*) as num_reg_total,
    sum(case when cjr.g5508_tasamora >= 0 and cjr.g5508_tasamora <= 100 then 0 else 1 end ) as num_reg_error,
    from_unixtime(unix_timestamp()) as op_timestamp
from bi_corp_bdr.jm_clien_juri cjr
where cjr.partition_date = '${partition_date}'
group by cjr.g5508_feoperac ;


--El importe Total Equity debe ser mayor o igual que cero (3.4.11)	R15123411
SELECT cjr.g5508_feoperac as fecha_proceso,
    'R15123411' as cod_incidencia,
    count(*) as num_reg_total,
    sum(case when cjr.g5508_tot_eqty >= 0  then 0 else 1 end ) as num_reg_error,
    from_unixtime(unix_timestamp()) as op_timestamp
from bi_corp_bdr.jm_clien_juri cjr  
where cjr.partition_date = '${partition_date}'
group by cjr.g5508_feoperac ;

--El valor del campo Marca empresa no obligada a tener facturación no es válido (3.4.11)	R15453411
SELECT cjr.g5508_feoperac as fecha_proceso,
    'R15453411' as cod_incidencia,
    count(*) as num_reg_total,
    sum(case when cjr.g5508_flgempno <> 'N' then 1 else 0 end ) as num_reg_error, 
    from_unixtime(unix_timestamp()) as op_timestamp
from bi_corp_bdr.jm_clien_juri cjr 
where cjr.partition_date = '${partition_date}'
group by cjr.g5508_feoperac ;