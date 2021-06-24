set mapred.job.queue.name=root.dataeng;
set hive.exec.dynamic.partition.mode=nonstrict;

WITH recaud_oper AS (

	SELECT ide_pgo
  		, ind_forma_pago 
  		, IF(TRIM(ind_forma_pago) = 'E', COUNT(1), 0) cantidad_efectivo
  		, IF(TRIM(ind_forma_pago) = 'E', SUM(imp_mov), 0) importe_efectivo
  		, IF(TRIM(ind_forma_pago) = 'C', COUNT(1), 0) cantidad_cheque
  		, IF(TRIM(ind_forma_pago) = 'C', SUM(imp_mov), 0) importe_cheque
  		, IF(TRIM(ind_forma_pago) NOT IN ('E','C'), COUNT(1), 0) cantidad_otros
  		, IF(TRIM(ind_forma_pago) NOT IN ('E','C'), SUM(imp_mov), 0) importe_otros
	FROM bi_corp_staging.rio61_test_inter_recaud_oper 
	WHERE partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Sucursales') }}'
    GROUP BY ide_pgo
  		, ind_forma_pago
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
	,CASE WHEN (TRIM(m.transaccion) = '003951' AND TRIM(r.ind_forma_pago) = '0') 
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
LEFT JOIN bi_corp_staging.rio30_agrupamiento a
	ON m.partition_date = a.partition_date
	AND TRIM(a.tx_id) = TRIM(m.transaccion) 
	AND TRIM(m.canal) = TRIM(a.canal_id)
LEFT JOIN bi_corp_common.stk_per_personas p
	ON p.partition_date = m.partition_date
	AND CAST(m.nup AS INT) = p.cod_per_nup
LEFT JOIN bi_corp_staging.rio30_transacciones t 
	ON TRIM(m.transaccion) = TRIM(t.tx_id) 
	AND t.partition_date = m.partition_date 
LEFT JOIN recaud_oper r 
	ON SUBSTRING(TRIM(m.datos), 1, 19) = r.ide_pgo
WHERE m.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Sucursales') }}'
UNION ALL -----------------------------------------------
    SELECT
    	DISTINCT TRIM(a.canal_id) cod_suc_canal
        , TRIM(a.cod_sector) cod_suc_sector
        , CAST(adde805.wacnl801_nup AS INT) cod_per_nup
        , TRIM(adde805.wacnl801_maquina) cod_suc_maquina
        , from_unixtime(unix_timestamp(CAST(adde805.wacnl801_fecha_amd AS string) ,'yyyyMMdd'), 'yyyy-MM-dd') dt_suc_fechatransaccion
        , from_unixtime(unix_timestamp(CONCAT(adde805.wacnl801_fecha_amd,' ',adde805.wacnl801_hora) ,'yyyyMMdd HHmmss'), 'yyyy-MM-dd HH:mm:ss') ts_suc_horatransaccion
        , adde805.wacnl801_tipo_cliente cod_per_tipopersona
        , CAST(adde805.wacnl801_sucursal_cuenta AS INT) cod_suc_sucursalcuenta
        , CAST(adde805.wacnl801_sucursal_maquina AS INT) cod_suc_sucursalmaquina
        , TRIM(CAST(adde805.wacnl801_transaccion AS string)) cod_suc_transaccion
        , TRIM(t.descripcion) ds_suc_transaccion
        , TRIM(adde805.wacnl801_ori_rev) cod_suc_originalreverso
        , TRIM(adde805.wacnl801_usuario) cod_suc_usuario
        , p.cod_per_cuadrante
        , p.ds_per_subsegmento
        , CAST(CAST(adde805.wacnl801_importe AS INT)/100 AS DECIMAL(14,2)) fc_suc_importetransaccion
        , TRIM(adde805.wacnl801_divisa) cod_div_divisa
        , COALESCE(r.cantidad_efectivo, 0) fc_suc_cantidadefectivo
        , COALESCE(r.importe_efectivo, 0) fc_suc_importeefectivo
        , CAST(adde805.wacnl801_cant_cheques AS INT) fc_suc_cantidadcheques
        , COALESCE(r.importe_cheque, 0) fc_suc_importecheques
        , COALESCE(r.cantidad_otros, 0) fc_suc_cantidadotros
        , COALESCE(r.importe_otros, 0) fc_suc_importeotros
        ,CASE WHEN (TRIM(cast(adde805.wacnl801_transaccion as string)) = '003951' AND TRIM(r.ind_forma_pago) = '0')
             OR  ((TRIM(cast(adde805.wacnl801_transaccion as string)) = '003934' OR TRIM(cast(adde805.wacnl801_transaccion as string)) = '013460') AND TRIM(r.ind_forma_pago) = '01') THEN 'E'
            WHEN (TRIM(cast(adde805.wacnl801_transaccion as string)) = '003951' AND TRIM(r.ind_forma_pago) = '1')
             OR ((TRIM(cast(adde805.wacnl801_transaccion as string)) = '003934' OR TRIM(cast(adde805.wacnl801_transaccion as string)) ='013460')
             AND TRIM(r.ind_forma_pago) IN ('02','03','04','05','06','07','08','09','10','12')) THEN 'C'
            WHEN (TRIM(cast(adde805.wacnl801_transaccion as string)) = '003951' AND TRIM(r.ind_forma_pago) = '2')
             OR (TRIM(cast(adde805.wacnl801_transaccion as string)) = '003934' OR TRIM(cast(adde805.wacnl801_transaccion as string)) = '013460') AND TRIM(r.ind_forma_pago) = '00' THEN 'D'
            ELSE TRIM(r.ind_forma_pago) END cod_suc_mediopago
        , TRIM(CAST(adde805.wacnl801_servicio_adom AS string)) cod_suc_enteid
        , TRIM(adde805.wacnl801_tipo_operacion) cod_suc_manualauto
        , TRIM(adde805.wacnl801_datos) cod_suc_idepago
        , IF(TRIM(a.cod_grupo) = 'null', NULL, TRIM(a.cod_grupo)) cod_suc_grupo
        , adde805.partition_date
    FROM bi_corp_staging.adde_adde805 adde805
    LEFT JOIN bi_corp_staging.rio30_agrupamiento a
	ON adde805.partition_date = a.partition_date
	AND TRIM(a.tx_id) = TRIM(CAST(adde805.wacnl801_transaccion AS string))
	AND TRIM(adde805.wacnl801_canal) = TRIM(a.canal_id)
LEFT JOIN bi_corp_common.stk_per_personas p
	ON p.partition_date = adde805.partition_date
	AND CAST(adde805.wacnl801_nup AS INT) = p.cod_per_nup
LEFT JOIN bi_corp_staging.rio30_transacciones t
	ON TRIM(CAST(adde805.wacnl801_transaccion AS string)) = TRIM(t.tx_id)
	AND t.partition_date = adde805.partition_date
LEFT JOIN recaud_oper r
	ON SUBSTRING(TRIM(adde805.wacnl801_datos), 1, 19) = r.ide_pgo
WHERE adde805.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Sucursales') }}'
UNION ALL -----------------------------------------------
    SELECT
    	DISTINCT TRIM(a.canal_id) cod_suc_canal
        , TRIM(a.cod_sector) cod_suc_sector
        , CAST(abae546.nup AS INT) cod_per_nup
        , TRIM(abae546.maquina) cod_suc_maquina
        , from_unixtime(unix_timestamp(CAST(abae546.fecha_amd AS string) ,'yyyyMMdd'), 'yyyy-MM-dd') dt_suc_fechatransaccion
        , from_unixtime(unix_timestamp(CONCAT(abae546.fecha_amd,' ',abae546.hora) ,'yyyyMMdd HHmmss'), 'yyyy-MM-dd HH:mm:ss') ts_suc_horatransaccion
        , abae546.tipo_cliente cod_per_tipopersona
        , CAST(abae546.sucursal_cuenta AS INT) cod_suc_sucursalcuenta
        , CAST(abae546.sucursal_maquina AS INT) cod_suc_sucursalmaquina
        , TRIM(CAST(abae546.transaccion AS string)) cod_suc_transaccion
        , TRIM(t.descripcion) ds_suc_transaccion
        , TRIM(abae546.ori_rev) cod_suc_originalreverso
        , TRIM(abae546.usuario) cod_suc_usuario
        , p.cod_per_cuadrante
        , p.ds_per_subsegmento
        , CAST(CAST(abae546.importe AS INT)/100 AS DECIMAL(14,2)) fc_suc_importetransaccion
        , TRIM(abae546.divisa) cod_div_divisa
        , COALESCE(r.cantidad_efectivo, 0) fc_suc_cantidadefectivo
        , COALESCE(r.importe_efectivo, 0) fc_suc_importeefectivo
        , CAST(abae546.cant_cheques AS INT) fc_suc_cantidadcheques
        , COALESCE(r.importe_cheque, 0) fc_suc_importecheques
        , COALESCE(r.cantidad_otros, 0) fc_suc_cantidadotros
        , COALESCE(r.importe_otros, 0) fc_suc_importeotros
        ,CASE WHEN (TRIM(cast(abae546.transaccion as string)) = '003951' AND TRIM(r.ind_forma_pago) = '0')
             OR  ((TRIM(cast(abae546.transaccion as string)) = '003934' OR TRIM(cast(abae546.transaccion as string)) = '013460') AND TRIM(r.ind_forma_pago) = '01') THEN 'E'
            WHEN (TRIM(cast(abae546.transaccion as string)) = '003951' AND TRIM(r.ind_forma_pago) = '1')
             OR ((TRIM(cast(abae546.transaccion as string)) = '003934' OR TRIM(cast(abae546.transaccion as string)) ='013460')
             AND TRIM(r.ind_forma_pago) IN ('02','03','04','05','06','07','08','09','10','12')) THEN 'C'
            WHEN (TRIM(cast(abae546.transaccion as string)) = '003951' AND TRIM(r.ind_forma_pago) = '2')
             OR (TRIM(cast(abae546.transaccion as string)) = '003934' OR TRIM(cast(abae546.transaccion as string)) = '013460') AND TRIM(r.ind_forma_pago) = '00' THEN 'D'
            ELSE TRIM(r.ind_forma_pago) END cod_suc_mediopago
        , TRIM(CAST(abae546.servicio_adom AS string)) cod_suc_enteid
        , TRIM(abae546.tipo_operacion) cod_suc_manualauto
        , TRIM(abae546.datos) cod_suc_idepago
        , IF(TRIM(a.cod_grupo) = 'null', NULL, TRIM(a.cod_grupo)) cod_suc_grupo
        , abae546.partition_date
FROM bi_corp_staging.abae_abae546 abae546
LEFT JOIN bi_corp_staging.rio30_agrupamiento a
ON abae546.partition_date = a.partition_date
AND TRIM(a.tx_id) = TRIM(CAST(abae546.transaccion AS string))
AND TRIM(abae546.canal) = TRIM(a.canal_id)
LEFT JOIN bi_corp_common.stk_per_personas p
	ON p.partition_date = abae546.partition_date
	AND CAST(abae546.nup AS INT) = p.cod_per_nup
LEFT JOIN bi_corp_staging.rio30_transacciones t
	ON TRIM(CAST(abae546.transaccion AS string)) = TRIM(t.tx_id)
	AND t.partition_date = abae546.partition_date
LEFT JOIN recaud_oper r
	ON SUBSTRING(TRIM(abae546.datos), 1, 19) = r.ide_pgo
WHERE abae546.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Sucursales') }}'
UNION ALL -----------------------------------------------
    SELECT
    	DISTINCT TRIM(a.canal_id) cod_suc_canal
        , TRIM(a.cod_sector) cod_suc_sector
        , CAST(abae547.nup AS INT) cod_per_nup
        , TRIM(abae547.maquina) cod_suc_maquina
        , from_unixtime(unix_timestamp(CAST(abae547.fecha_amd AS string) ,'yyyyMMdd'), 'yyyy-MM-dd') dt_suc_fechatransaccion
        , from_unixtime(unix_timestamp(CONCAT(abae547.fecha_amd,' ',abae547.hora) ,'yyyyMMdd HHmmss'), 'yyyy-MM-dd HH:mm:ss') ts_suc_horatransaccion
        , abae547.tipo_cliente cod_per_tipopersona
        , CAST(abae547.sucursal_cuenta AS INT) cod_suc_sucursalcuenta
        , CAST(abae547.sucursal_maquina AS INT) cod_suc_sucursalmaquina
        , TRIM(CAST(abae547.transaccion AS string)) cod_suc_transaccion
        , TRIM(t.descripcion) ds_suc_transaccion
        , TRIM(abae547.ori_rev) cod_suc_originalreverso
        , TRIM(abae547.usuario) cod_suc_usuario
        , p.cod_per_cuadrante
        , p.ds_per_subsegmento
        , CAST(CAST(abae547.importe AS INT)/100 AS DECIMAL(14,2)) fc_suc_importetransaccion
        , TRIM(abae547.divisa) cod_div_divisa
        , COALESCE(r.cantidad_efectivo, 0) fc_suc_cantidadefectivo
        , COALESCE(r.importe_efectivo, 0) fc_suc_importeefectivo
        , CAST(abae547.cant_cheques AS INT) fc_suc_cantidadcheques
        , COALESCE(r.importe_cheque, 0) fc_suc_importecheques
        , COALESCE(r.cantidad_otros, 0) fc_suc_cantidadotros
        , COALESCE(r.importe_otros, 0) fc_suc_importeotros
        ,CASE WHEN (TRIM(cast(abae547.transaccion as string)) = '003951' AND TRIM(r.ind_forma_pago) = '0')
             OR  ((TRIM(cast(abae547.transaccion as string)) = '003934' OR TRIM(cast(abae547.transaccion as string)) = '013460') AND TRIM(r.ind_forma_pago) = '01') THEN 'E'
            WHEN (TRIM(cast(abae547.transaccion as string)) = '003951' AND TRIM(r.ind_forma_pago) = '1')
             OR ((TRIM(cast(abae547.transaccion as string)) = '003934' OR TRIM(cast(abae547.transaccion as string)) ='013460')
             AND TRIM(r.ind_forma_pago) IN ('02','03','04','05','06','07','08','09','10','12')) THEN 'C'
            WHEN (TRIM(cast(abae547.transaccion as string)) = '003951' AND TRIM(r.ind_forma_pago) = '2')
             OR (TRIM(cast(abae547.transaccion as string)) = '003934' OR TRIM(cast(abae547.transaccion as string)) = '013460') AND TRIM(r.ind_forma_pago) = '00' THEN 'D'
            ELSE TRIM(r.ind_forma_pago) END cod_suc_mediopago
        , TRIM(CAST(abae547.servicio_adom AS string)) cod_suc_enteid
        , TRIM(abae547.tipo_operacion) cod_suc_manualauto
        , TRIM(abae547.datos) cod_suc_idepago
        , IF(TRIM(a.cod_grupo) = 'null', NULL, TRIM(a.cod_grupo)) cod_suc_grupo
        , abae547.partition_date
FROM bi_corp_staging.abae_abae547 abae547
LEFT JOIN bi_corp_staging.rio30_agrupamiento a
ON abae547.partition_date = a.partition_date
AND TRIM(a.tx_id) = TRIM(CAST(abae547.transaccion AS string))
AND TRIM(abae547.canal) = TRIM(a.canal_id)
LEFT JOIN bi_corp_common.stk_per_personas p
	ON p.partition_date = abae547.partition_date
	AND CAST(abae547.nup AS INT) = p.cod_per_nup
LEFT JOIN bi_corp_staging.rio30_transacciones t
	ON TRIM(CAST(abae547.transaccion AS string)) = TRIM(t.tx_id)
	AND t.partition_date = abae547.partition_date
LEFT JOIN recaud_oper r
	ON SUBSTRING(TRIM(abae547.datos), 1, 19) = r.ide_pgo
WHERE abae547.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Sucursales') }}'
UNION ALL -----------------------------------------------
    SELECT
    	DISTINCT TRIM(a.canal_id) cod_suc_canal
        , TRIM(a.cod_sector) cod_suc_sector
        , CAST(abae549.nup AS INT) cod_per_nup
        , TRIM(abae549.maquina) cod_suc_maquina
        , from_unixtime(unix_timestamp(CAST(abae549.fecha_amd AS string) ,'yyyyMMdd'), 'yyyy-MM-dd') dt_suc_fechatransaccion
        , from_unixtime(unix_timestamp(CONCAT(abae549.fecha_amd,' ',abae549.hora) ,'yyyyMMdd HHmmss'), 'yyyy-MM-dd HH:mm:ss') ts_suc_horatransaccion
        , abae549.tipo_cliente cod_per_tipopersona
        , CAST(abae549.sucursal_cuenta AS INT) cod_suc_sucursalcuenta
        , CAST(abae549.sucursal_maquina AS INT) cod_suc_sucursalmaquina
        , TRIM(CAST(abae549.transaccion AS string)) cod_suc_transaccion
        , TRIM(t.descripcion) ds_suc_transaccion
        , TRIM(abae549.ori_rev) cod_suc_originalreverso
        , TRIM(abae549.usuario) cod_suc_usuario
        , p.cod_per_cuadrante
        , p.ds_per_subsegmento
        , CAST(CAST(abae549.importe AS INT)/100 AS DECIMAL(14,2)) fc_suc_importetransaccion
        , TRIM(abae549.divisa) cod_div_divisa
        , COALESCE(r.cantidad_efectivo, 0) fc_suc_cantidadefectivo
        , COALESCE(r.importe_efectivo, 0) fc_suc_importeefectivo
        , CAST(abae549.cant_cheques AS INT) fc_suc_cantidadcheques
        , COALESCE(r.importe_cheque, 0) fc_suc_importecheques
        , COALESCE(r.cantidad_otros, 0) fc_suc_cantidadotros
        , COALESCE(r.importe_otros, 0) fc_suc_importeotros
        ,CASE WHEN (TRIM(cast(abae549.transaccion as string)) = '003951' AND TRIM(r.ind_forma_pago) = '0')
             OR  ((TRIM(cast(abae549.transaccion as string)) = '003934' OR TRIM(cast(abae549.transaccion as string)) = '013460') AND TRIM(r.ind_forma_pago) = '01') THEN 'E'
            WHEN (TRIM(cast(abae549.transaccion as string)) = '003951' AND TRIM(r.ind_forma_pago) = '1')
             OR ((TRIM(cast(abae549.transaccion as string)) = '003934' OR TRIM(cast(abae549.transaccion as string)) ='013460')
             AND TRIM(r.ind_forma_pago) IN ('02','03','04','05','06','07','08','09','10','12')) THEN 'C'
            WHEN (TRIM(cast(abae549.transaccion as string)) = '003951' AND TRIM(r.ind_forma_pago) = '2')
             OR (TRIM(cast(abae549.transaccion as string)) = '003934' OR TRIM(cast(abae549.transaccion as string)) = '013460') AND TRIM(r.ind_forma_pago) = '00' THEN 'D'
            ELSE TRIM(r.ind_forma_pago) END cod_suc_mediopago
        , TRIM(CAST(abae549.servicio_adom AS string)) cod_suc_enteid
        , TRIM(abae549.tipo_operacion) cod_suc_manualauto
        , TRIM(abae549.datos) cod_suc_idepago
        , IF(TRIM(a.cod_grupo) = 'null', NULL, TRIM(a.cod_grupo)) cod_suc_grupo
        , abae549.partition_date
FROM bi_corp_staging.abae_abae549 abae549
LEFT JOIN bi_corp_staging.rio30_agrupamiento a
ON abae549.partition_date = a.partition_date
AND TRIM(a.tx_id) = TRIM(CAST(abae549.transaccion AS string))
AND TRIM(abae549.canal) = TRIM(a.canal_id)
LEFT JOIN bi_corp_common.stk_per_personas p
	ON p.partition_date = abae549.partition_date
	AND CAST(abae549.nup AS INT) = p.cod_per_nup
LEFT JOIN bi_corp_staging.rio30_transacciones t
	ON TRIM(CAST(abae549.transaccion AS string)) = TRIM(t.tx_id)
	AND t.partition_date = abae549.partition_date
LEFT JOIN recaud_oper r
	ON SUBSTRING(TRIM(abae549.datos), 1, 19) = r.ide_pgo
WHERE abae549.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Sucursales') }}'
