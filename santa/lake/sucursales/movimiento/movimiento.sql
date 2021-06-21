set hive.execution.engine=spark;
set mapred.job.queue.name=root.dataeng;
set hive.exec.dynamic.partition.mode=nonstrict;

WITH recaud_oper AS (

  SELECT ide_pgo
  		, ind_forma_pago 
  		, IF(TRIM(ind_forma_pago) = 'E', COUNT(1) OVER (PARTITION BY ide_pgo), 0) cantidad_efectivo
  		, IF(TRIM(ind_forma_pago) = 'E', SUM(imp_mov) OVER (PARTITION BY ide_pgo), 0) importe_efectivo
  		, IF(TRIM(ind_forma_pago) = 'C', COUNT(1) OVER (PARTITION BY ide_pgo), 0) cantidad_cheque
  		, IF(TRIM(ind_forma_pago) = 'C', SUM(imp_mov) OVER (PARTITION BY ide_pgo), 0) importe_cheque
  		, IF(TRIM(ind_forma_pago) NOT IN ('E','C'), COUNT(1) OVER (PARTITION BY ide_pgo), 0) cantidad_otros
  		, IF(TRIM(ind_forma_pago) NOT IN ('E','C'), SUM(imp_mov) OVER (PARTITION BY ide_pgo), 0) importe_otros
  FROM bi_corp_staging.rio61_test_inter_recaud_oper 
  WHERE partition_date = date'2021-03-25'

	)

INSERT overwrite TABLE bi_corp_common.bt_suc_movimientos PARTITION (partition_date)

SELECT DISTINCT TRIM(a.canal_id) cod_suc_canal
	, TRIM(a.cod_sector) cod_suc_sector
	, CAST(m.nup AS INT) cod_per_nup
	, TRIM(m.maquina) cod_suc_maquina
	, from_unixtime(unix_timestamp(m.fecha_amd ,'yyyyMMdd'), 'yyyy-MM-dd') dt_suc_fechatransaccion
	, from_unixtime(unix_timestamp(CONCAT(m.fecha_amd,' ',m.hora) ,'yyyyMMdd HHmmss'), 'yyyy-MM-dd HH:mm:ss') ts_suc_horatransaccion
	, m.tipo_cliente cod_per_tipopersona
	, CAST(m.sucursal_cuenta AS INT) cod_suc_sucursalcuenta
	, CAST(m.sucursal_maquina AS INT) cod_suc_sucursalmaquina
	, TRIM(m.transaccion) cod_suc_transaccion
	, TRIM(t.descripcion) ds_suc_transaccion
	, TRIM(m.ori_rev) cod_suc_originalreverso
	, TRIM(m.usuario) cod_suc_usuario
	, p.cod_per_cuadrante 
	, p.ds_per_subsegmento 
	, CAST(CAST(m.importe AS INT)/100 AS DECIMAL(14,2)) fc_suc_importetransaccion
	, TRIM(m.divisa) cod_div_divisa
	, COALESCE(r.cantidad_efectivo, 0) fc_suc_cantidadefectivo 
	, COALESCE(r.importe_efectivo, 0) fc_suc_importeefectivo
	, CAST(m.cant_cheques AS INT) fc_suc_cantidadcheques
	, COALESCE(r.importe_cheque, 0) fc_suc_importecheques
	, COALESCE(r.cantidad_otros, 0) fc_suc_cantidadotros 
	, COALESCE(r.importe_otros, 0) fc_suc_importeotros
	, CASE WHEN (TRIM(m.transaccion) = '003951' AND TRIM(r.ind_forma_pago) = '0') 
		 OR  ((TRIM(m.transaccion) = '003934' OR TRIM(m.transaccion) = '013460') AND TRIM(r.ind_forma_pago) = '01') THEN 'E'
		WHEN (TRIM(m.transaccion) = '003951' AND TRIM(r.ind_forma_pago) = '1') 
		 OR ((TRIM(m.transaccion) = '003934' OR TRIM(m.transaccion) ='013460')
		 AND TRIM(r.ind_forma_pago) IN ('02','03','04','05','06','07','08','09','10','12')) THEN 'C'
		WHEN (TRIM(m.transaccion) = '003951' AND TRIM(r.ind_forma_pago) = '2')
		 OR (TRIM(m.transaccion) = '003934' OR TRIM(m.transaccion) = '013460') AND TRIM(r.ind_forma_pago) = '00' THEN 'D' 
		ELSE TRIM(r.ind_forma_pago) END cod_suc_mediopago
	, TRIM(m.servicio_adom) cod_suc_enteid
	, TRIM(m.tipo_operacion) cod_suc_manualauto
	, TRIM(m.datos) cod_suc_idepago
	, IF(TRIM(a.cod_grupo) = 'null', NULL, TRIM(a.cod_grupo)) cod_suc_grupo
	, m.partition_date 
FROM bi_corp_staging.acnl_acnl801d m
JOIN bi_corp_staging.rio30_agrupamiento a
	ON m.partition_date = a.partition_date
	AND TRIM(a.tx_id) = TRIM(m.transaccion) 
	AND TRIM(m.canal) = TRIM(a.canal_id)
	AND TRIM(m.servicio_adom) = TRIM(a.ente_id)
LEFT JOIN bi_corp_common.stk_per_personas p
	ON p.partition_date = m.partition_date
	AND CAST(m.nup AS INT) = p.cod_per_nup
LEFT JOIN bi_corp_staging.rio30_transacciones t 
	ON TRIM(m.transaccion) = TRIM(t.tx_id) 
	AND t.partition_date = m.partition_date 
LEFT JOIN recaud_oper r 
	ON SUBSTRING(TRIM(m.datos), 1, 19) = r.ide_pgo
WHERE m.partition_date = '2021-03-25' ;

