


SELECT *
FROM bi_corp_staging.acnl_acnl801d
WHERE partition_date  = '2021-03-15'
AND TRIM(transaccion) = '080804'

SELECT *
FROM bi_corp_staging.acnl_acnl801d 
WHERE partition_date  = '2021-03-15'
AND TRIM(canal) = 'CAJA'
and TRIM(maquina)  between '8000' and '8099';

DESCRIBE bi_corp_staging.rio30_agrupamiento ;
-- cod_sector
-- canal_id
-- tx_id
-- ente_id
-- cod_grupo
-- informa_marca
-- fecha_informa

DESCRIBE bi_corp_staging.rio30_transacciones ;

DESCRIBE bi_corp_staging.acnl_acnl801d ; 


-- /_/_/_/_/_/_/_/_/_/_/_/_/_/_/_/_/_/_/_/_/_/_/_/_/

SELECT CAST(m.nup AS INT) nup
	, m.maquina--m.MAQ_ID,
	, TO_CHAR(m.fecha_amd,'YYYY-MM-DD') fecha
	, m.hora
	, m.tipo_cliente 
	, m.sucursal_cuenta --m.SUC_NRO,
	, m.sucursal_maquina--m.SUC_MAQ,
	, m.transaccion --m.TX_ID,
	, m.ori_rev 
	, m.usuario 
	, cod_per_cuadrante cuadrante
	, CAST(CAST(m.importe AS INT)/100 AS DECIMAL(14,2)) importe
	, m.divisa moneda
	, CAST(m.cant_cheques AS INT) cantidad_cheques
	, p.ds_per_subsegmento pesubseg
	
	--m.CANTIDAD_EFECTIVO, m.IMPORTE_EFECTIVO, m.IMPORTE_CHEQUES,
	--m.CANTIDAD_OTROS, m.IMPORTE_OTROS, m.MEDIO_PAGO
	
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
WHERE m.partition_date = '2021-03-15'
	AND SUBSTRING(TRIM(m.maquina), 1,2) = '80'
	AND TRIM(m.sucursal_maquina) = '492'
	ORDER BY
	m.sucursal_maquina, 
	m.maquina, 
	m.fecha_amd, 
	m.hora ;

-- /_/_/_/_/_/_/_/_/_/_/_/_/_/_/_/_/_/_/_/_/_/_/_/_/


SELECT a.ide_pgo
     , cantidad_efectivo
     , importe_efectivo
     , cantidad_cheque
     , importe_cheque
     , cantidad_otros
     , importe_otros
     , NVL(b.importe_efectivo,0)+ NVL(c.importe_cheque,0) + NVL(d.importe_otros,0) importe
FROM
     (SELECT ide_pgo
      FROM TEST_INTER_RECAUD_OPER
      WHERE fecha_proceso = l_fecha_proceso
      GROUP BY ide_pgo
      ) A
   , (SELECT  ide_pgo
             ,COUNT(*)     cantidad_efectivo
             ,SUM(imp_mov) importe_efectivo
      FROM TEST_INTER_RECAUD_OPER
      WHERE fecha_proceso  = l_fecha_proceso
        AND ind_forma_pago = 'E'
      GROUP BY ide_pgo
      ) B
   , (SELECT  ide_pgo
             ,COUNT(*)     cantidad_cheque
             ,SUM(imp_mov) importe_cheque
      FROM TEST_INTER_RECAUD_OPER
      WHERE fecha_proceso  = l_fecha_proceso
        AND ind_forma_pago = 'C'
      GROUP BY ide_pgo
      ) C
   , (SELECT  ide_pgo
             ,COUNT(*)     cantidad_otros
             ,SUM(imp_mov) importe_otros
      FROM TEST_INTER_RECAUD_OPER
      WHERE fecha_proceso  = l_fecha_proceso
        AND ind_forma_pago NOT IN ('E','C')
      GROUP BY ide_pgo
      ) D
WHERE a.ide_pgo       = b.ide_pgo(+)
  AND a.ide_pgo       = c.ide_pgo(+)
  AND a.ide_pgo       = d.ide_pgo(+)
;

-- /_/_/_/_/_/_/_/_/_/_/_/_/_/_/_/_/_/_/_/_/_/_/_/_/


SELECT * FROM bi_corp_staging.rio61_test_inter_recaud_oper


SELECT DISTINCT cod_sector FROM bi_corp_staging.rio30_agrupamiento a
WHERE partition_date = '2021-03-15'
AND TRIM(a.cod_sector) = 'CG'
AND TRIM(a.canal_id) = 'CAJA' ;

SELECT DISTINCT id_bcaaut_mensaje
FROM bi_corp_common.bt_bcaaut_respuestas
WHERE partition_date = '2021-03-16'

SHOW PARTITIONS bi_corp_common.bt_bcaaut_respuestas












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
	WHERE m.partition_date = '2020-10-31'

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
	, IF(TRIM(a.cod_grupo) = 'null', NULL, TRIM(a.cod_grupo))
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


SELECT * FROM bi_corp_common.bt_suc_movimientos WHERE partition_date = '2020-10-11'

SHOW PARTITIONS bi_corp_common.bt_suc_movimientos 

SHOW PARTITIONS  bi_corp_staging.rio30_movimiento 

SHOW PARTITIONS bi_corp_staging.rio30_agrupamiento 


SELECT * 
FROM bi_corp_risk.cartera_ajustes 
WHERE partition_date = '2021-04-12' ORDER BY periodo DESC ;


SELECT * FROM bi_corp_staging.adde_adde805



SELECT * 
FROM bi_corp_staging.rio157_ms0_ft_dwh_blce_result
WHERE partition_date = '2021-01-31' AND cod_entidad_espana = '1024'  ;















INSERT OVERWRITE TABLE bi_corp_common.dim_rec_usuario
SELECT
	TRIM(usu.id_usuario) cod_rec_usuario ,
	TRIM(usu.apellido) ds_rec_usuario_apellido ,
	TRIM(usu.nombre) ds_rec_usuario_nombre ,
	CASE WHEN LENGTH(TRIM(usu.email))=0 THEN NULL ELSE TRIM(usu.email) END as ds_rec_usuario_email ,
	CASE WHEN LENGTH(TRIM(usu.guid_tibco))=0 THEN NULL ELSE TRIM(usu.guid_tibco) END as cod_rec_guid_tibco ,
	TRIM(usu.id_usuario_alta) cod_usuario_alta ,
	to_date(usu.fecha_alta) dt_rec_usuario_alta,
	to_date(usu.fecha_baja) dt_rec_usuario_baja,
	TRIM(usu.id_usuario_modif) cod_usuario_modif ,
	to_date(usu.fecha_modif) dt_rec_usuario_modif,
	TRIM(usu.cod_sucursal) cod_suc_sucursal ,
	TRIM(usu.cod_oficina) cod_rec_oficina,
	usu.partition_date
FROM
	bi_corp_staging.rio187_usuarios usu
WHERE partition_date = '2021-04-14'
AND id_usuario = 'B038128' ;


SELECT * FROM bi_corp_common.dim_rec_usuario WHERE cod_rec_usuario='B038128'

SELECT * FROM
	bi_corp_staging.rio187_usuarios usu
WHERE partition_date = '2021-04-14'
AND id_usuario = 'B038128' ;

MSCK REPAIR TABLE bi_corp_common.dim_rec_usuario ;

/*
 Table(tableName:dim_rec_usuario
 , dbName:bi_corp_common, owner:hive
 , createTime:1618527063, lastAccessTime:0, retention:0
 , sd:StorageDescriptor(cols:
 [FieldSchema(name:cod_rec_usuario, type:string, comment:null), FieldSchema(name:ds_rec_usuario_apellido, 
 type:string, comment:null), FieldSchema(name:ds_rec_usuario_nombre, type:string, comment:null), 
 FieldSchema(name:ds_rec_usuario_email, type:string, comment:null), FieldSchema(name:cod_rec_guid_tibco, type:string, comment:null),
  FieldSchema(name:cod_usuario_alta, type:string, comment:null), FieldSchema(name:dt_rec_usuario_alta, type:timestamp, comment:null), 
  FieldSchema(name:dt_rec_usuario_baja, type:timestamp, comment:null), FieldSchema(name:cod_usuario_modif, type:string, comment:null), 
  FieldSchema(name:dt_rec_usuario_modif, type:timestamp, comment:null), FieldSchema(name:cod_suc_sucursal, type:int, comment:null),
   FieldSchema(name:cod_rec_oficina, type:string, comment:null), 
   FieldSchema(name:partition_date, type:string, comment:null)], 
   location:hdfs://namesrvprod/santander/bi-corp/common/reclamos/dim_rec_usuario, 
   inputFormat:org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat,
    outputFormat:org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat, 
    compressed:false, numBuckets:-1, serdeInfo:SerDeInfo(name:null, 
    serializationLib:org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe, parameters:{serialization.format=1}), 
    bucketCols:[], sortCols:[], parameters:{}, skewedInfo:SkewedInfo(skewedColNames:[], skewedColValues:[], 
    skewedColValueLocationMaps:{}), storedAsSubDirectories:false), partitionKeys:[], parameters:{totalSize=0, numRows=7959,
     rawDataSize=103467, EXTERNAL=TRUE, COLUMN_STATS_ACCURATE=true, numFiles=0, transient_lastDdlTime=1618579227}, 
     viewOriginalText:null, viewExpandedText:null, tableType:EXTERNAL_TABLE)
 * */




2018-04-17 00:00:00		COSMOS	2018-04-17 00:00:00
2018-04-27 00:00:00		B038128	2019-02-12 00:00:00








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
	WHERE partition_date = date'2021-04-14'

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
	, IF(TRIM(a.cod_grupo) = 'null', NULL, TRIM(a.cod_grupo)) cod_suc_grupo
	, m.partition_date 
FROM bi_corp_staging.acnl_acnl801d m
JOIN bi_corp_staging.rio30_agrupamiento a
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
WHERE m.partition_date = '2021-04-14' ;














SHOW PARTITIONS bi_corp_staging.rio61_test_inter_recaud_oper ;
DESCRIBE bi_corp_common.bt_suc_movimientos ;
SELECT * FROM bi_corp_common.bt_suc_movimientos WHERE partition_date = '2021-04-16';
SHOW PARTITIONS bi_corp_common.bt_suc_movimientos ;
DESCRIBE bi_corp_staging.acnl_acnl801d ;
SHOW PARTITIONS bi_corp_staging.acnl_acnl801d ;

--partition_date=2021-03-26 .
--partition_date=2021-03-29 .
--partition_date=2021-03-30 .
--partition_date=2021-03-31 .
--partition_date=2021-04-05 .
--partition_date=2021-04-06 
--partition_date=2021-04-07 
--partition_date=2021-04-08 
--partition_date=2021-04-09 .
--partition_date=2021-04-12 .
--partition_date=2021-04-13 .
--partition_date=2021-04-14 .
--partition_date=2021-04-15 .
--partition_date=2021-04-16 .
--partition_date=2021-04-19 .
--partition_date=2021-04-20 .


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
	WHERE partition_date = '2021-04-06'

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
	
LEFT JOIN bi_corp_common.stk_per_personas p
	ON p.partition_date = m.partition_date
	AND CAST(m.nup AS INT) = p.cod_per_nup
LEFT JOIN bi_corp_staging.rio30_transacciones t 
	ON TRIM(m.transaccion) = TRIM(t.tx_id) 
	AND t.partition_date = m.partition_date 
LEFT JOIN recaud_oper r 
	ON SUBSTRING(TRIM(m.datos), 1, 19) = r.ide_pgo
WHERE m.partition_date = '2021-04-06' ;















