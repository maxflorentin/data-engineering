--            ___               ___   _     __    _   
--   /\/\    /___\/\   /\      / __\ /_\    \ \  /_\  
--  /    \  //  //\ \ / /____ / /   //_\\    \ \//_\\ 
-- / /\/\ \/ \_//  \ V /_____/ /___/  _  \/\_/ /  _  \
-- \/    \/\___/    \_/      \____/\_/ \_/\___/\_/ \_/
                                                   
set hive.execution.engine=spark;
set mapred.job.queue.name=root.dataeng;
set hive.exec.dynamic.partition.mode=nonstrict;

SELECT CAST(m.nup AS INT) cod_per_nup
	, TRIM(m.maquina) cod_suc_maquina
	, from_unixtime(unix_timestamp(m.fecha_amd ,'yyyyMMdd'), 'yyyy-MM-dd') dt_suc_fechatransaccion
	, m.hora 
	, m.tipo_cliente cod_per_tipopersona
	, CAST(m.sucursal_cuenta AS INT) cod_suc_sucursalcuenta
	, CAST(m.sucursal_maquina AS INT) cod_suc_sucursalmaquina
	, m.transaccion cod_suc_transaccion --TX_ID
	, t.descripcion ds_suc_transaccion
	, m.ori_rev cod_suc_originalreverso
	, m.usuario cod_suc_usuario
	, cod_per_cuadrante cod_per_cuadrante
	, p.ds_per_subsegmento ds_per_subsegmento
	, CAST(CAST(m.importe AS INT)/100 AS DECIMAL(14,2)) fc_suc_importetransaccion
	, TRIM(m.divisa) cod_div_divisa
	, CAST(m.cant_cheques AS INT) fc_suc_cantidadcheques
	
	--, r.cantidad_efectivo
	--, r.importe_efectivo
	--, r.importe_cheques
	--, r.cantidad_otros
	--, r.importe_otros
	--, r.medio_pago	
FROM bi_corp_staging.acnl_acnl801d m
JOIN bi_corp_staging.rio30_agrupamiento a
	ON m.partition_date = a.partition_date
	AND TRIM(a.cod_sector) = 'CG'
	AND TRIM(a.canal_id) = 'CAJA'
	AND TRIM(a.tx_id) = TRIM(m.transaccion) 
	AND TRIM(m.canal) = TRIM(a.canal_id)
LEFT JOIN bi_corp_common.stk_per_personas p
	ON p.partition_date = m.partition_date
	AND CAST(m.nup AS INT) = p.cod_per_nup
LEFT JOIN bi_corp_staging.rio30_transacciones t 
	ON TRIM(m.transaccion) = TRIM(t.tx_id) 
	AND t.partition_date = m.partition_date 
WHERE m.partition_date = '2021-03-15'
	AND SUBSTRING(TRIM(m.maquina), 1,2) = '80'
	AND TRIM(m.sucursal_maquina) = '492';
	

