set mapred.job.queue.name=root.dataeng;
set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=4000000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.auto.convert.join=true;
set hive.auto.convert.join.noconditionaltask=true;
set hive.auto.convert.join.noconditionaltask.size=40000000;

WITH acnl801 AS (

	SELECT m.canal_id 
		, m.nup
		, m.maq_id 
		, m.fecha
		, m.hora
		, m.tipo_cliente
		, m.suc_nro
		, m.suc_maq
		, m.tx_id
		, m.orig_rev
		, m.usuario
		, IF(TRIM(m.cuadrante) = 'null', 'I', TRIM(m.cuadrante)) cuadrante
		, IF(TRIM(m.pesubseg) = 'null', 'I', TRIM(m.pesubseg)) pesubseg
		, m.importe
		, m.moneda
		, m.cantidad_efectivo
		, m.importe_efectivo
		, m.cantidad_cheques
		, m.importe_cheques
		, m.cantidad_otros
		, m.importe_otros
		, m.medio_pago
		, m.ente_id
		, m.marca_man_aut 
		, m.ide_pago
		, SUBSTRING(m.fecha, 1, 10) p_date
	FROM bi_corp_staging.rio30_movimiento m
	WHERE m.partition_date = '2020-12-31'

)

INSERT overwrite TABLE bi_corp_common.bt_suc_movimientos PARTITION (partition_date)
SELECT DISTINCT TRIM(m.canal_id) cod_suc_canal
	, TRIM(a.cod_sector) cod_suc_sector
	, CAST(m.nup AS INT) cod_per_nup
	, TRIM(m.maq_id) cod_suc_maquina
	, SUBSTRING(m.fecha, 1, 10) dt_suc_fechatransaccion
	, from_unixtime(unix_timestamp(CONCAT(SUBSTRING(m.fecha, 1, 10),' ',m.hora) ,'yyyy-MM-dd HHmmss'), 'yyyy-MM-dd HH:mm:ss') ts_suc_horatransaccion
	, TRIM(m.tipo_cliente) cod_per_tipopersona
	, CAST(m.suc_nro AS INT) cod_suc_sucursalcuenta
	, CAST(m.suc_maq AS INT) cod_suc_sucursalmaquina
	, TRIM(m.tx_id) cod_suc_transaccion
	, TRIM(t.descripcion) ds_suc_transaccion
	, TRIM(m.orig_rev) cod_suc_originalreverso
	, TRIM(m.usuario) cod_suc_usuario
	, TRIM(m.cuadrante) cod_per_cuadrante 
	, TRIM(m.pesubseg) ds_per_subsegmento
	, COALESCE(m.importe, 0)fc_suc_importetransaccion
	, IF(TRIM(m.moneda) = 'null', NULL, TRIM(m.moneda)) cod_div_divisa
	, COALESCE(CAST(m.cantidad_efectivo AS INT), 0) fc_suc_cantidadefectivo 
	, IF(TRIM(m.importe_efectivo) = 'null', 0, TRIM(m.importe_efectivo)) fc_suc_importeefectivo
	, CAST(m.cantidad_cheques AS INT) fc_suc_cantidadcheques
	, IF(TRIM(m.importe_cheques) = 'null', 0, TRIM(m.importe_cheques))fc_suc_importecheques
	, COALESCE(CAST(m.cantidad_otros AS INT), 0) fc_suc_cantidadotros 
	, IF(TRIM(m.importe_otros) = 'null', 0, TRIM(m.importe_otros)) fc_suc_importeotros
	, IF(TRIM(m.medio_pago) = 'null', NULL, TRIM(m.medio_pago)) cod_suc_mediopago
	, IF(TRIM(m.ente_id) = 'null', NULL, TRIM(m.ente_id)) cod_suc_enteid
	, IF(TRIM(m.marca_man_aut) = 'null', NULL, TRIM(m.marca_man_aut)) cod_suc_manualauto
	, IF(TRIM(m.ide_pago) = 'null', NULL, TRIM(m.ide_pago)) cod_suc_idepago
	, IF(TRIM(a.cod_grupo) = 'null', NULL, TRIM(a.cod_grupo)) cod_suc_grupo
	, m.p_date partition_date
FROM acnl801 m 
JOIN bi_corp_staging.rio30_agrupamiento a
		ON m.p_date = a.partition_date
		AND TRIM(a.tx_id) = TRIM(m.tx_id) 
		AND TRIM(m.canal_id) = TRIM(a.canal_id)
		AND TRIM(m.ente_id) = TRIM(a.ente_id)
	LEFT JOIN bi_corp_staging.rio30_transacciones t 
		ON TRIM(m.tx_id) = TRIM(t.tx_id) 
		AND t.partition_date = m.p_date ;
