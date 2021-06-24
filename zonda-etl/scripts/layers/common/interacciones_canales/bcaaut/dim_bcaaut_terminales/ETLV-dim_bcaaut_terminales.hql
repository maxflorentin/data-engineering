"
set mapred.job.queue.name=root.dataeng;
set hive.exec.dynamic.partition.mode=nonstrict;
-----------------------------------------------
INSERT OVERWRITE TABLE bi_corp_common.dim_bcaaut_terminales
PARTITION(partition_date) 
SELECT TRIM(xe.sigla) cod_bcaaut_sigla
	, TRIM(xe.nro_serie) ds_bcaaut_nroserie
	, TRIM(xe.equipo) ds_bcaaut_tipoequipo
	, TRIM(em.modelo_descri) ds_bcaaut_modelo
	, TRIM(ema.marca_descri) ds_bcaaut_marca
	, TRIM(xe.apertura) ds_bcaaut_apertura
	, IF(TRIM(xe.reconoc_bill) = 'TRUE', 1, 0) flag_bcaaut_reconocebilletes
	, TRIM(xe.funcionalidad) ds_bcaaut_funcionalidad
	, CAST(pnum.posicion_num AS INT) cod_bcaaut_sucursal
	, TRIM(regexp_replace(p.nombre, '¿', '')) ds_bcaaut_sucursal
	, TRIM(p.numero_id) cod_bcaaut_sucursalcc -- TRIM(pnum2.posicion_num)
	, TRIM(pt.descri_pos_tipo) ds_bcaaut_posiciontipo
	, TRIM(sz.zona) cod_bcaaut_zona
	, IF(TRIM(z.zona_nombre) = 'null', NULL, TRIM(z.zona_nombre)) ds_bcaaut_zona
	, IF(TRIM(pd.calle) = 'null', NULL, TRIM(pd.calle)) ds_bcaaut_sucursalcalle
	, IF(TRIM(pd.numero) = 'null', NULL, TRIM(pd.numero)) ds_bcaaut_sucursalnro
	, IF(TRIM(pd.postal) = 'null', NULL, TRIM(pd.postal)) ds_bcaaut_sucursalpostal
	, IF(TRIM(pd.postal_cpa) = 'null', NULL, TRIM(pd.postal_cpa)) ds_bcaaut_sucursalpostalcc
	, IF(TRIM(pd.tel) = 'null', NULL, TRIM(pd.tel)) ds_bcaaut_sucursaltelefono
	, IF(TRIM(loc.descripcion) = 'null', NULL, TRIM(loc.descripcion)) ds_bcaaut_sucursallocalidad
	, IF(TRIM(prov.descripcion) = 'null', NULL, TRIM(prov.descripcion)) ds_bcaaut_sucursalprovincia
	, IF(TRIM(eo.operador_descrip) = 'null', NULL, TRIM(eo.operador_descrip)) ds_bcaaut_operador
	, IF(TRIM(xe.equipo) IN ('ATM','TOT'), 'PRISMA', IF(TRIM(tas.tas_prisma) = 'SI', 'PRISMA', 'ISBAN')) ds_bcaaut_red
	, xe.partition_date
FROM bi_corp_staging.rio44_ba_equipos xe
-------------------------------------------------------- modelo
	LEFT JOIN bi_corp_staging.rio44_ba_equipos_modelo em
		ON TRIM(xe.modelo) = TRIM(em.id_modelo)
		AND em.partition_date = xe.partition_date 
-------------------------------------------------------- marca
	LEFT JOIN bi_corp_staging.rio44_ba_equipos_marca ema 
		ON TRIM(em.marca_id) = TRIM(ema.id_marca)
		AND ema.partition_date = xe.partition_date
-------------------------------------------------------- sucursal		
	LEFT JOIN bi_corp_staging.rio44_ba_equipos_posicion ep 
		ON TRIM(xe.id_equipos) = TRIM(ep.id_equipos)
		AND ep.f_baja = 'null'
		AND ep.partition_date = xe.partition_date
		-----------------------------------------
	LEFT JOIN bi_corp_staging.rio44_ba_posicion p 
		ON TRIM(ep.id_posicion) = TRIM(p.id_posicion) --  TRIM(p.numero_id)
		--AND p.activo = 'S'
		AND p.partition_date = xe.partition_date
	LEFT JOIN bi_corp_staging.rio44_ba_posicion_num pnum 
		ON TRIM(ep.id_posicion) = TRIM(pnum.id_posicion_num)
		AND pnum.partition_date = xe.partition_date
	-----------------------------------------------	
	LEFT JOIN bi_corp_staging.rio44_ba_posicion_num pnum2
		ON TRIM(p.numero_id_cont) = TRIM(pnum2.id_posicion_num)
		AND pnum2.partition_date = xe.partition_date
-------------------------------------------------------- zona
	LEFT JOIN bi_corp_staging.rio44_ba_suc_zona sz
		ON TRIM(p.id_posicion) = TRIM(sz.suc)
		AND sz.partition_date = xe.partition_date
	LEFT JOIN bi_corp_staging.rio44_ba_zonal z
		ON TRIM(sz.zona) = TRIM(z.id_zona)
		AND z.partition_date = xe.partition_date
-------------------------------------------------------- domicilio_suc
	LEFT JOIN bi_corp_staging.rio44_ba_posicion_domicilio pd
		ON TRIM(p.id_posicion) = TRIM(pd.posicion_id)
		AND pd.partition_date = xe.partition_date
	LEFT JOIN bi_corp_staging.rio44_rpt_d_localidad loc
		ON TRIM(pd.localidad_id) = TRIM(loc.localidadsid)
		-- AND loc.partition_date = '2020-12-10'
	LEFT JOIN bi_corp_staging.rio44_ba_provincia prov
		ON TRIM(pd.provincia_id) = TRIM(prov.id_provincia)
		AND prov.partition_date = xe.partition_date
--------------------------------------------------------- operador
	LEFT JOIN bi_corp_staging.rio44_ba_equipos_operador eo
		ON TRIM(xe.operador) = TRIM(eo.id_operador)
		AND eo.partition_date = xe.partition_date
--------------------------------------------------------- posiciontipo
	LEFT JOIN bi_corp_staging.rio44_ba_posicion_tipo pt
		ON TRIM(p.posicion_tipo_id) = TRIM(pt.id_posicion_tipo)
		AND pt.partition_date = xe.partition_date
--------------------------------------------------------- red
	LEFT JOIN bi_corp_staging.rio44_ba_equipos_tas tas 
		ON TRIM(xe.id_equipos) = TRIM(tas.id_equipo)
 	AND xe.partition_date = tas.partition_date
--------------------------------------------------------
WHERE xe.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_BCAAUT_Terminales') }}'
	AND TRIM(xe.id_sed) IN ('7','56','55') ;
"