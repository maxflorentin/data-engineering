

set hive.execution.engine=spark;
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
WHERE xe.partition_date = {{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_BCAAUT_Terminales-Common') }}
	AND TRIM(xe.id_sed) IN ('7','56','55') ;




-- DESCRIBE santander_business_risk_arda.contratos_adsf
SELECT * FROM santander_business_risk_arda.contratos_adsf 
-- DESCRIBE santander_business_risk_arda.contratos_adsf
SELECT * FROM santander_business_risk_arda.contratos_deudores_adic 

-- DESCRIBE bi_corp_staging.rio155_atm_remanentes 



CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_common.stk_bcaaut_remanentes (
	ts_bcaaut_fecha	string ,
	cod_bcaaut_sigla	string ,
	cod_bcaaut_sucursal	int ,
	ds_bcaaut_sucursal	string ,
	cod_bcaaut_zona	string ,
	ds_bcaaut_operador	string ,
	ds_bcaaut_posiciontipo	string ,
	ds_bcaaut_tipoequipo	string ,
	fc_bcaaut_cargado	decimal(16,2) ,
	fc_bcaaut_dispensado	decimal(16,2) ,
	fc_bcaaut_remanente	decimal(16,2) )
PARTITIONED BY(partition_date string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '/santander/bi-corp/common/interacciones_canales/bcaaut/fact/stk_bcaaut_remanentes'	


set hive.execution.engine=spark;
set mapred.job.queue.name=root.dataeng;
set hive.exec.dynamic.partition.mode=nonstrict;
-----------------------------------------------
INSERT OVERWRITE TABLE bi_corp_common.stk_bcaaut_remanentes
PARTITION(partition_date)  
SELECT 
   SUBSTRING(RM.fecha, 1, 19) ts_bcaaut_fecha
 , RM.terminal cod_bcaaut_sigla
 , TM.cod_bcaaut_sucursal cod_bcaaut_sucursal
 , TM.ds_bcaaut_sucursal ds_bcaaut_sucursal
 , TM.cod_bcaaut_zona
 , TM.ds_bcaaut_operador
 , TM.ds_bcaaut_posiciontipo 
 , TM.ds_bcaaut_tipoequipo ds_bcaaut_tipoequipo
 , NULL fc_bcaaut_cargado
 , NULL fc_bcaaut_dispensado
 , CAST(RM.remanente AS INT) fc_bcaaut_remanente
 , RM.partition_date 
FROM bi_corp_staging.rio155_atm_remanentes RM
	LEFT JOIN bi_corp_common.dim_bcaaut_terminales TM
		ON TM.cod_bcaaut_sigla = TRIM(RM.terminal)
		AND TM.partition_date = RM.partition_date 
WHERE RM.partition_date = '2020-12-17'


SELECT DISTINCT cod_bcaaut_sigla FROM bi_corp_common.stk_bcaaut_remanentes WHERE partition_date = '2020-12-17' ;



CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.nesr_encolador_d(
	id_tkt string,
	id_suc string,
	sector_espera string,
	apellidocliente string,
	nombrecliente string,
	cod_producto string,
	cod_motivo string,
	mejorproducto string,
	segmento string,
	semafororentabilidad string,
	semafororenta string,
	productosegmento string,
	contacto string,
	tipodoc string,
	nrodoc string,
	subsegmento string,
	nup string,
	fechaticket string,
	campana string,
	numero_servicio string,
	tipoatencion string,
	legajo string,
	canalcomp string,
	fecha_atencion string,
	oficial_atencion string,
	id_gestion string,
	fecha_proceso string,
	procesado string,
	fecha_fin_atencion string,
	nombre_servicio string,
	motivo_enc string,
	oficial_nombre string,
	origen string,
	estado string,
	motivo_abandono string,
	tecn string )
PARTITIONED BY(partition_date string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '/santander/bi-corp/staging/nesr/fact/encolador_d' ;



CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.nesr_encolador_det_ticket (
	fecha string,
	sucursal string,
	zona string,
	tipo_cola string,
	ticket string,
	numero_servicio string,
	nombre_servicio string,
	numero_puesto string,
	nombre_puesto string,
	usuario string,
	t_obj_espera string,
	t_max_espera string,
	fecha_ingreso string,
	fecha_llamado string,
	fecha_fin_atencion string,
	tiempo_espera string,
	tiempo_atencion string,
	auxiliar1 string,
	auxiliar2 string,
	cod_motivo string,
	nom_motivo string,
	estado string,
	motivo_abandono string,
	tecn string )
PARTITIONED BY(partition_date string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '/santander/bi-corp/staging/nesr/fact/encolador_det_ticket' ;




--------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------


SELECT * FROM santander_business_risk_arda.saldos_altair_mes_actual WHERE data_date_part = '20191023' LIMIT 100 ;

SELECT --FECHA
	, cod_entidad cod_cys_entidad -- ENTIDAD
	-- RUBRO CONTABLE LOCAL
	-- DESCRIPCION DEL RUBRO
	-- TIPO DE RUBRO
	-- RUBRO CARGABAL
	-- RUBRO BCRA
	-- NIIF
	-- PLAN DE NEGOCIOS
	-- CUENTA_ACTD
	, cod_divisa cod_cys_divisa -- DIVISA
	-- CUENTA_REGULARIZADORA
	-- SALDO PROMEDIO A LA FECHA
	-- SALDO PROMEDIO A LA FECHA AJUSTADO POR INFLACION
	-- SALDO CIERRE AÑO ANTERIOR (DICIEMBRE AÑO ANTERIOR)
	-- ENERO
	-- FEBRERO
	-- MARZO
	-- ABRIL
	-- MAYO
	-- JUNIO
	-- JULIO
	-- AGOSTO
	-- SEPTIEMBRE
	-- OCTUBRE
	-- NOVIEMBRE
	-- DICIEMBRE
	-- SALDO CIERRE AÑO ANTERIOR (DICIEMBRE AÑO ANTERIOR) AJUSTADO POR INFLACION
	-- ENERO AJUSTADO POR INFLACION
	-- FEBRERO AJUSTADO POR INFLACION
	-- MARZO AJUSTADO POR INFLACION
	-- ABRIL AJUSTADO POR INFLACION
	-- MAYO AJUSTADO POR INFLACION
	-- JUNIO AJUSTADO POR INFLACION
	-- JULIO AJUSTADO POR INFLACION
	-- AGOSTO AJUSTADO POR INFLACION
	-- SEPTIEMBRE AJUSTADO POR INFLACION
	-- OCTUBRE AJUSTADO POR INFLACION
	-- NOVIEMBRE AJUSTADO POR INFLACION
	-- DICIEMBRE AJUSTADO POR INFLACION
	
 
 SELECT * FROM bi_corp_common.bt_rec_gestiones_sgc WHERE cod_rec_gestion_nro = 19432192	
 
 
SHOW PARTITIONS bi_corp_staging.alha9601

DESCRIBE EXTENDED bi_corp_staging.alha9601

SELECT * FROM bi_corp_staging.alha9601 WHERE partition_date='2020-12-04' LIMIT 10 

"Table(tableName:alha9601, dbName:bi_corp_staging, owner:srvcengineerbi, createTime:1598043201, lastAccessTime:0, retention:0, sd:StorageDescriptor(cols:[FieldSchema(name:entidad, type:string, comment:null), FieldSchema(name:fecha_informacion, type:string, comment:null), FieldSchema(name:rubro_altair, type:string, comment:null), FieldSchema(name:nombre_cuenta, type:string, comment:null), FieldSchema(name:rubro_bcra, type:string, comment:null), FieldSchema(name:cargabal, type:string, comment:null), FieldSchema(name:pdn, type:string, comment:null), FieldSchema(name:em, type:string, comment:null), FieldSchema(name:cuenta_ant, type:string, comment:null), FieldSchema(name:rubro_niif, type:string, comment:null), FieldSchema(name:enero, type:string, comment:null), FieldSchema(name:febrero, type:string, comment:null), FieldSchema(name:marzo, type:string, comment:null), FieldSchema(name:abril, type:string, comment:null), FieldSchema(name:mayo, type:string, comment:null), FieldSchema(name:junio, type:string, comment:null), FieldSchema(name:julio, type:string, comment:null), FieldSchema(name:agosto, type:string, comment:null), FieldSchema(name:septiembre, type:string, comment:null), FieldSchema(name:octubre, type:string, comment:null), FieldSchema(name:noviembre, type:string, comment:null), FieldSchema(name:diciembre, type:string, comment:null), FieldSchema(name:partition_date, type:string, comment:null)], location:hdfs://namesrvprod/santander/bi-corp/staging/manual/contab/alha9601, inputFormat:org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat, outputFormat:org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat, compressed:false, numBuckets:-1, serdeInfo:SerDeInfo(name:null, serializationLib:org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe, parameters:{serialization.format=1}), bucketCols:[], sortCols:[], parameters:{}, skewedInfo:SkewedInfo(skewedColNames:[], skewedColValues:[], skewedColValueLocationMaps:{}), storedAsSubDirectories:false), partitionKeys:[FieldSchema(name:partition_date, type:string, comment:null)], parameters:{EXTERNAL=TRUE, numPartitions=6, transient_lastDdlTime=1598043201}, viewOriginalText:null, viewExpandedText:null, tableType:EXTERNAL_TABLE)"






SELECT * FROM bi_corp_risk.puntajes_seguimiento LIMIT 10 ;

-- ENC_ESTADO
-- MOTIVO_VISITA_V2
-- SUCURSALES
-- SUCURSALRIO
-- ENC_PUESTO_CITAS 
-- ENC_TURNO
-- ENC_DURACION

-- DESCRIBE bi_corp_staging.rio154_enc_turno 

CREATE TEMPORARY TABLE sucursales AS
SELECT CAST(ET.id_turno AS BIGINT) cod_suc_turno
	, CAST(ET.id_estado AS INT) cod_suc_estado
	, TRIM(EE.descri) ds_suc_estado
	, IF(TRIM(ET.tipodoc) = 'null', NULL, TRIM(ET.tipodoc)) ds_suc_tipodoc
	, IF(TRIM(ET.nrodoc) = 'null', NULL, CAST(ET.nrodoc AS BIGINT)) ds_per_numdoc
	, IF(TRIM(ET.sector) = 'P', 'PLATAFORMA', IF(TRIM(ET.sector) = 'C', 'CAJA', NULL)) ds_suc_sector
	, CAST(ET.id_suc AS INT) cod_suc_sucursal
	, TRIM(SUC.descri) ds_suc_sucursal
	, ET.fecha_hora_desde
	, ET.fecha_hora_hasta
	, TRIM(ET.fraccion) ds_suc_fraccion
	, TRIM(ET.dia) ds_suc_dia
	, IF(TRIM(ET.nup) = 'null', NULL, CAST(ET.nup AS BIGINT)) cod_per_nup  
	, IF(TRIM(ET.apellido) = 'null', NULL, TRIM(ET.apellido)) ds_per_apellido
	, IF(TRIM(ET.nombre) = 'null', NULL, TRIM(ET.nombre)) ds_per_nombre
	, IF(TRIM(ET.pesubseg) = 'null', NULL, TRIM(ET.pesubseg)) cod_suc_subseg  
	, IF(TRIM(ET.pesexper) = 'null', NULL, TRIM(ET.pesexper)) cod_per_sexo
	, CAST(ET.id_motivo AS INT) cod_suc_motivovisita
	, TRIM(MV.descripcion) ds_suc_motivovisita
	, IF(TRIM(ET.mail) = 'null', NULL, TRIM(ET.mail)) ds_suc_mail
	, IF(TRIM(ET.cuit) = 'null', NULL, CAST(ET.cuit AS BIGINT)) ds_per_cuit
	, IF(TRIM(ET.id_turno_orig) = 'null', NULL, CAST(ET.id_turno_orig AS INT)) cod_suc_turnooriginal
	, IF(TRIM(ET.id_ejec_atencion) = 'null', NULL, TRIM(ET.id_ejec_atencion)) cod_suc_ejecatencion
	, IF(TRIM(ET.id_suc_atencion) = 'null', NULL, TRIM(ET.id_suc_atencion)) cod_suc_sucursalatencion
	, ET.id_puesto_citas
	, IF(TRIM(ET.telefono) = 'null', NULL, TRIM(ET.telefono)) ds_suc_telefono
	, ET.id_tipo_turno
	, SUBSTRING(ET.fecha_desde, 1, 19) ts_suc_fechadesde
	, SUBSTRING(ET.fecha_hasta, 1, 19) ts_suc_fechahasta
	, SUBSTRING(ET.fecha_hasta, 1, 10) partition_date
FROM bi_corp_staging.rio154_enc_turno ET 
-------------------------------------------ESTADO
LEFT JOIN bi_corp_staging.rio154_enc_estado EE 
	ON ET.id_estado = EE.id_estado
LEFT JOIN bi_corp_staging.rio154_motivo_visita_v2 MV 
	ON ET.id_motivo = MV.id_motivo
LEFT JOIN bi_corp_staging.rio154_sucursalrio SUC 
	ON ET.id_suc = SUC.codigo ;
---------------------------------------------------------------------
-- DROP TABLE bi_corp_common.bt_suc_turnero ;
CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_common.bt_suc_turnero ( 
	cod_suc_turno	bigint ,
	cod_suc_estado	int ,
	ds_suc_estado	string ,
	ds_suc_tipodoc	string ,
	ds_per_numdoc	bigint ,
	ds_suc_sector	string ,
	cod_suc_sucursal	int ,
	ds_suc_sucursal	string ,
	fecha_hora_desde	string ,
	fecha_hora_hasta	string ,
	ds_suc_fraccion	string ,
	ds_suc_dia	string ,
	cod_per_nup	bigint ,
	ds_per_apellido	string ,
	ds_per_nombre	string ,
	cod_suc_subseg	string ,
	cod_per_sexo	string ,
	cod_suc_motivovisita	int ,
	ds_suc_motivovisita	string ,
	ds_suc_mail	string ,
	ds_per_cuit	bigint ,
	cod_suc_turnooriginal	int ,
	cod_suc_ejecatencion	string ,
	cod_suc_sucursalatencion	string ,
	cod_suc_puestocitas	string ,
	ds_suc_telefono	string ,
	cod_suc_tipoturno	string ,
	ds_suc_tipoturno    string ,
	ts_suc_fechadesde	string ,
	ts_suc_fechahasta	string ) 
PARTITIONED BY ( partition_date string) 
STORED AS PARQUET 
LOCATION '/santander/bi-corp/common/interacciones_canales/sucursal/fact/bt_suc_turnero'




SET hive.execution.engine = spark;
SET spark.yarn.queue = root.dataeng;
SET hive.exec.dynamic.partition.mode = nonstrict;
--------------------------------------------------
INSERT overwrite TABLE bi_corp_common.bt_suc_turnero PARTITION (partition_date)
--------------------------------------------------
SELECT CAST(ET.id_turno AS BIGINT) cod_suc_turno
	, CAST(ET.id_estado AS INT) cod_suc_estado
	, TRIM(EE.descri) ds_suc_estado
	, DS.ds_tipodoc ds_suc_tipodoc
	, IF(TRIM(ET.nrodoc) = 'null', NULL, CAST(ET.nrodoc AS BIGINT)) ds_per_numdoc
	, IF(TRIM(ET.sector) = 'P', 'PLATAFORMA', IF(TRIM(ET.sector) = 'C', 'CAJA', NULL)) ds_suc_sector
	, CAST(ET.id_suc AS INT) cod_suc_sucursal
	, TRIM(SUC.descri) ds_suc_sucursal
	, ET.fecha_hora_desde
	, ET.fecha_hora_hasta
	, TRIM(ET.fraccion) ds_suc_fraccion
	, TRIM(ET.dia) ds_suc_dia
	, IF(TRIM(ET.nup) = 'null', NULL, CAST(ET.nup AS BIGINT)) cod_per_nup  
	, IF(TRIM(ET.apellido) = 'null', NULL, TRIM(ET.apellido)) ds_per_apellido
	, IF(TRIM(ET.nombre) = 'null', NULL, TRIM(ET.nombre)) ds_per_nombre
	, IF(TRIM(ET.pesubseg) = 'null', NULL, TRIM(ET.pesubseg)) cod_suc_subseg  
	, IF(TRIM(ET.pesexper) = 'null', NULL, TRIM(ET.pesexper)) cod_per_sexo
	, CAST(ET.id_motivo AS INT) cod_suc_motivovisita
	, TRIM(MV.descripcion) ds_suc_motivovisita
	, IF(TRIM(ET.mail) = 'null', NULL, TRIM(ET.mail)) ds_suc_mail
	, IF(TRIM(ET.cuit) = 'null', NULL, CAST(ET.cuit AS BIGINT)) ds_per_cuit
	, IF(TRIM(ET.id_turno_orig) = 'null', NULL, CAST(ET.id_turno_orig AS INT)) cod_suc_turnooriginal
	, IF(TRIM(ET.id_ejec_atencion) = 'null', NULL, TRIM(ET.id_ejec_atencion)) cod_suc_ejecatencion
	, IF(TRIM(ET.id_suc_atencion) = 'null', NULL, TRIM(ET.id_suc_atencion)) cod_suc_sucursalatencion
	, TRIM(ET.id_puesto_citas) cod_suc_puestocitas
	, IF(TRIM(ET.telefono) = 'null', NULL, TRIM(ET.telefono)) ds_suc_telefono
	, TRIM(ET.id_tipo_turno) cod_suc_tipoturno
	, IF(TRIM(ET.id_tipo_turno) = '1', 'FISICO', IF(TRIM(ET.id_tipo_turno) = '2', 'REMOTO', NULL)) ds_suc_tipoturno
	, SUBSTRING(ET.fecha_desde, 1, 19) ts_suc_fechadesde
	, SUBSTRING(ET.fecha_hasta, 1, 19) ts_suc_fechahasta
	, SUBSTRING(ET.fecha_hasta, 1, 10) partition_date
FROM bi_corp_staging.rio154_enc_turno ET 
-------------------------------------------------------ESTADO
LEFT JOIN bi_corp_staging.rio154_enc_estado EE 
	ON ET.id_estado = EE.id_estado
	AND ET.partition_date = EE.partition_date 
-------------------------------------------------------MOTIVO
LEFT JOIN bi_corp_staging.rio154_motivo_visita_v2 MV 
	ON ET.id_motivo = MV.id_motivo
	AND ET.partition_date = MV.partition_date 
-------------------------------------------------------SUCURSAL
LEFT JOIN bi_corp_staging.rio154_sucursalrio SUC 
	ON ET.id_suc = SUC.codigo 
	AND ET.partition_date = SUC.partition_date 
-------------------------------------------------------DS_TIPODOC
LEFT JOIN 
	(SELECT DISTINCT TRIM(substr(gen_clave, 1, 1)) tipodoc, gen_datos ds_tipodoc
	 FROM bi_corp_staging.tcdtgen WHERE gentabla = '0113') DS
	ON TRIM(ET.tipodoc) = DS.tipodoc
------------------------------------------------------
WHERE ET.partition_date = '2021-01-05' ;	


SELECT * FROM bi_corp_common.bt_suc_turnero LIMIT 100 ;

SELECT cod_suc_turno FROM bi_corp_common.bt_suc_turnero
GROUP BY cod_suc_turno HAVING COUNT(cod_suc_turno) > 1 ;

SELECT *
FROM bi_corp_common.bt_suc_turnero 
where partition_date = '2021-01-06' and cod_suc_estado = 55;



SELECT * FROM bi_corp_staging.nesr_encolador_d WHERE partition_date = '2020-12-28'


-- DROP TABLE bi_corp_staging.afir_in00 ;


	
	



set hive.execution.engine=spark;
set mapred.job.queue.name=root.dataeng;
set hive.exec.dynamic.partition.mode=nonstrict;
-----------------------------------------------
INSERT OVERWRITE TABLE bi_corp_common.stk_cys_inhabilitados
PARTITION(partition_date)
SELECT * FROM (
SELECT CAST(INH.nro_inh AS BIGINT) cod_cys_inh
	, TRIM(INH.tpo_pers) cod_per_tipopersona 
	, TRIM(INH.ape_pers) ds_per_apellido
	, TRIM(INH.nom_pers) ds_per_nombre
	, TRIM(INH.cod_sex_pers) cod_per_sexo
	, TRIM(INH.fec_naci) dt_per_fechanacimiento
	, TRIM(DS.ds_tipodoc) ds_per_tipodoc
	, CAST(INH.nro_doc AS BIGINT) ds_per_numdoc
	, CAST(INH.cod_caus AS INT) cod_cys_causal
	, CAST(INH.sec_caus AS INT) int_cys_seccaus
	, TRIM(INH.fec_inh) dt_cys_inh
	, IF(TRIM(INH.fec_rehb) = '3011-11-27', NULL, TRIM(INH.fec_rehb)) dt_cys_rehb
	, IF(TRIM(INH.vigencia) = 'S', 1, 0) flag_cys_vigencia
	, last_day(to_date(INH.fec_inh)) partition_date
FROM bi_corp_staging.afir_in00 INH 
LEFT JOIN 
	(SELECT DISTINCT TRIM(substr(gen_clave, 1, 1)) tipodoc, gen_datos ds_tipodoc
	 FROM bi_corp_staging.tcdtgen WHERE gentabla = '0113') DS
	ON TRIM(INH.tpo_doc) = DS.tipodoc 
WHERE partition_date = '2021-01-26' 
) A ;

	



SELECT * FROM bi_corp_common.stk_cys_inhabilitados WHERE partition_date='2021-01-25' LIMIT 100 ;





	
SELECT * FROM bi_corp_staging.zror_activo WHERE cod_ren_contrato = '00720041390071039100525966000' ;



CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_common.stk_cys_inhabilitados ( 
	cod_cys_inh	bigint ,
	cod_per_tipopersona	string ,
	ds_per_apellido	string ,
	ds_per_nombre	string ,
	cod_per_sexo	string ,
	dt_per_fechanacimiento	string ,
	ds_per_tipodoc	string ,
	ds_per_numdoc	bigint ,
	cod_cys_causal	int ,
	int_cys_seccaus	int ,
	dt_cys_inh	string ,
	dt_cys_rehb	string ,
	flag_cys_vigencia	int )
PARTITIONED BY ( partition_date string) 
STORED AS PARQUET 
LOCATION '/santander/bi-corp/common/cys/stk_cys_inhabilitados'

SET hive.exec.dynamic.partition.mode=nonstrict;
SET mapred.job.queue.name=root.dataeng;
-----------------------------------------------
INSERT OVERWRITE TABLE bi_corp_common.stk_cys_inhabilitados
PARTITION(partition_date)
SELECT CAST(INH.nro_inh AS BIGINT) cod_cys_inh
	, TRIM(INH.tpo_pers) cod_per_tipopersona 
	, TRIM(INH.ape_pers) ds_per_apellido
	, TRIM(INH.nom_pers) ds_per_nombre
	, TRIM(INH.cod_sex_pers) cod_per_sexo
	, TRIM(INH.fec_naci) dt_per_fechanacimiento
	, TRIM(DS.ds_tipodoc) ds_per_tipodoc
	, CAST(INH.nro_doc AS BIGINT) ds_per_numdoc
	, CAST(INH.cod_caus AS INT) cod_cys_causal
	, CAST(INH.sec_caus AS INT) int_cys_seccaus
	, TRIM(INH.fec_inh) dt_cys_inh
	, IF(TRIM(INH.fec_rehb) = '3011-11-27', NULL, TRIM(INH.fec_rehb)) dt_cys_rehb
	, IF(TRIM(INH.vigencia) = 'S', 1, 0) flag_cys_vigencia
	, last_day(to_date(INH.fec_inh)) partition_date
FROM bi_corp_staging.afir_in00 INH 
LEFT JOIN 
	(SELECT DISTINCT TRIM(substr(gen_clave, 1, 1)) tipodoc, gen_datos ds_tipodoc
	 FROM bi_corp_staging.tcdtgen WHERE gentabla = '0113') DS
	ON TRIM(INH.tpo_doc) = DS.tipodoc 
WHERE INH.partition_date = '2021-01-26' ;

-- SELECT * FROM bi_corp_common.stk_cys_inhabilitados ;

DESCRIBE bi_corp_staging.zror_activo_01 ;

		   
-- DESCRIBE bi_corp_staging.zror_pasivo		   
-- SELECT COUNT(1) FROM bi_corp_common.bt_ror_input_pasivo WHERE partition_date = '2020-11-30' ; --  23.925.892
-- SELECT COUNT(1) FROM bi_corp_staging.zror_pasivo ; 											 -- 243.049.803


-- SELECT COUNT(1) FROM bi_corp_common.bt_ror_input_activo WHERE partition_date = '2020-11-30' ; -- 132.509.120
-- SELECT COUNT(1) FROM bi_corp_staging.zror_activo_ws ; 										 -- 356.795.442

SELECT * FROM bi_corp_common.bt_ror_input_activo WHERE partition_date = '2020-11-30' LIMIT 50 ;
SELECT * FROM bi_corp_common.bt_ror_input_pasivo WHERE partition_date = '2020-11-30' LIMIT 50 ;




SET hive.merge.mapfiles=true;
SET hive.merge.mapredfiles=true;
SET hive.merge.size.per.task=4000000;
SET hive.merge.smallfiles.avgsize=16000000;
SET hive.exec.dynamic.partition.mode=nonstrict;
SET mapred.job.queue.name=root.dataeng;
-----------------------------------------------
INSERT OVERWRITE TABLE bi_corp_common.stk_cys_inhabilitados
PARTITION(partition_date)
SELECT * FROM inh 
WHERE partition_date = '2020-12-31';

SELECT * FROM bi_corp_common.stk_cys_inhabilitados ;

SET hive.exec.dynamic.partition.mode=nonstrict;
SET mapred.job.queue.name=root.dataeng;
DROP TABLE inh ;
CREATE TEMPORARY TABLE inh AS 
SELECT * FROM (
SELECT CAST(INH.nro_inh AS BIGINT) cod_cys_inh
	, TRIM(INH.tpo_pers) cod_per_tipopersona 
	, TRIM(INH.ape_pers) ds_per_apellido
	, TRIM(INH.nom_pers) ds_per_nombre
	, TRIM(INH.cod_sex_pers) cod_per_sexo
	, TRIM(INH.fec_naci) dt_per_fechanacimiento
	, TRIM(DS.ds_tipodoc) ds_per_tipodoc
	, CAST(INH.nro_doc AS BIGINT) ds_per_numdoc
	, TRIM(INH.cod_caus) cod_cys_causal
	, TRIM(INH.sec_caus) int_cys_seccaus
	, TRIM(INH.fec_inh) dt_cys_inh
	, IF(TRIM(INH.fec_rehb) = '3011-11-27' OR TRIM(INH.fec_rehb) = '9999-12-31', NULL, TRIM(INH.fec_rehb)) dt_cys_rehb
	, IF(TRIM(INH.vigencia) = 'S', 1, 0) flag_cys_vigencia
	, to_date(INH.fec_inh) partition_date
FROM bi_corp_staging.afir_in00 INH 
LEFT JOIN 
	(SELECT DISTINCT TRIM(substr(gen_clave, 1, 1)) tipodoc, gen_datos ds_tipodoc
	 FROM bi_corp_staging.tcdtgen WHERE gentabla = '0113') DS
	ON TRIM(INH.tpo_doc) = DS.tipodoc 
WHERE INH.partition_date > '2021-02-11' 
) A ORDER BY partition_date ;
SELECT * FROM inh ;


DESCRIBE bi_corp_staging.afir_in00 ;

set hive.execution.engine=spark;
set spark.yarn.queue=root.dataeng; 
SET hive.exec.dynamic.partition.mode=nonstrict;

WITH inhabilitados AS (
SELECT CAST(in00.nro_inh AS BIGINT) cod_cys_inh
	, TRIM(in00.tpo_pers) cod_per_tipopersona 
	, TRIM(in00.ape_pers) ds_per_apellido
	, TRIM(in00.nom_pers) ds_per_nombre
	, TRIM(in00.cod_sex_pers) cod_per_sexo
	, TRIM(in00.fec_naci) dt_per_fechanacimiento
	, TRIM(DS.ds_tipodoc) ds_per_tipodoc
	, CAST(in00.nro_doc AS BIGINT) ds_per_numdoc
	, TRIM(in00.cod_caus) cod_cys_causal
	, TRIM(in00.sec_caus) int_cys_seccaus
	, TRIM(in00.fec_inh) dt_cys_inh
	, IF(TRIM(in00.fec_rehb) > TRIM(in00.fec_rehb), NULL, TRIM(in00.fec_rehb)) dt_cys_rehb
	, IF(TRIM(in00.vigencia) = 'S', 1, 0) flag_cys_vigencia
	, to_date(in00.fec_inh) partition_date
FROM bi_corp_staging.afir_in00 in00 
LEFT JOIN 
	(SELECT DISTINCT TRIM(substr(gen_clave, 1, 1)) tipodoc, gen_datos ds_tipodoc
	 FROM bi_corp_staging.tcdtgen WHERE gentabla = '0113') DS
	ON in00.tpo_doc = DS.tipodoc 
WHERE in00.partition_date = '2021-01-26' 
ORDER BY to_date(in00.fec_inh))

INSERT OVERWRITE TABLE bi_corp_common.stk_cys_inhabilitados
PARTITION(partition_date)
SELECT * FROM inhabilitados WHERE partition_date > '1989-12-31' ;

----------------------------------------------------------------------------------------------------------------

-- DROP TABLE bi_corp_common.stk_cys_inhabilitados ;

CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_common.stk_cys_inhabilitados ( 
	cod_cys_inh	bigint ,
	cod_per_tipopersona	string ,
	ds_per_apellido	string ,
	ds_per_nombre	string ,
	cod_per_sexo	string ,
	dt_per_fechanacimiento	string ,
	ds_per_tipodoc	string ,
	ds_per_numdoc	bigint ,
	cod_cys_causal	string ,
	ds_cys_causal string ,
	int_cys_seccaus	int ,
	dt_cys_inh	string ,
	dt_cys_rehb	string ,
	flag_cys_vigencia	int ,
	int_cys_diasantiguedad int )
PARTITIONED BY ( partition_date string) 
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '/santander/bi-corp/common/cys/stk_cys_inhabilitados'



SET hive.exec.dynamic.partition.mode=nonstrict;
SET mapred.job.queue.name=root.dataeng;
----------------------------------------------
INSERT OVERWRITE TABLE bi_corp_common.stk_cys_inhabilitados
PARTITION(partition_date)
SELECT DISTINCT CAST(INH.nro_inh AS BIGINT) cod_cys_inh
	, TRIM(INH.tpo_pers) cod_per_tipopersona 
	, TRIM(INH.ape_pers) ds_per_apellido
	, TRIM(INH.nom_pers) ds_per_nombre
	, TRIM(INH.cod_sex_pers) cod_per_sexo
	, TRIM(INH.fec_naci) dt_per_fechanacimiento
	, TRIM(DS.ds_tipodoc) ds_per_tipodoc
	, CAST(INH.nro_doc AS BIGINT) ds_per_numdoc
	, TRIM(INH.cod_caus) cod_cys_causal
	, TRIM(CA.ds_caus) ds_cys_causal
	, TRIM(INH.sec_caus) int_cys_seccaus
	, TRIM(INH.fec_inh) dt_cys_inh
	, TRIM(INH.fec_rehb) dt_cys_rehb
	, IF(TRIM(INH.vigencia) = 'S', 1, 0) flag_cys_vigencia
	, IF(TRIM(INH.vigencia) = 'S', datediff(to_date(INH.partition_date), to_date(INH.fec_inh)), NULL) int_cys_antiguedad
	, INH.partition_date
FROM bi_corp_staging.afir_in00 INH 
LEFT JOIN 
	(SELECT DISTINCT TRIM(substr(gen_clave, 1, 1)) tipodoc, gen_datos ds_tipodoc
	 FROM bi_corp_staging.tcdtgen WHERE gentabla = '0113') DS
	ON TRIM(INH.tpo_doc) = DS.tipodoc 
LEFT JOIN bi_corp_staging.afir_cod_causales CA 
	ON TRIM(INH.cod_caus) = TRIM(CA.cod_caus)
WHERE INH.partition_date > '2021-02-11'  ;


SELECT * FROM bi_corp_common.stk_cys_inhabilitados 
WHERE partition_date = '2021-02-11'
AND cod_cys_causal IN ('017','018','019') ;

-- SHOW PARTITIONS bi_corp_staging.afir_in00
--------------------------------------------------------------------------

CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_common.stk_cys_inhabilitadosrisk ( 
	cod_cys_inh	bigint ,
	cod_per_tipopersona	string ,
	ds_per_apellido	string ,
	ds_per_nombre	string ,
	cod_per_sexo	string ,
	dt_per_fechanacimiento	string ,
	ds_per_tipodoc	string ,
	ds_per_numdoc	bigint ,
	cod_cys_causal	string ,
	ds_cys_causal string ,
	int_cys_seccaus	int ,
	dt_cys_inh	string ,
	dt_cys_rehb	string ,
	flag_cys_vigencia	int ,
	int_cys_diasantiguedad int )
PARTITIONED BY ( partition_date string) 
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '/santander/bi-corp/common/cys/stk_cys_inhabilitadosrisk'

SET hive.exec.dynamic.partition.mode=nonstrict;
SET mapred.job.queue.name=root.dataeng;
set hive.exec.max.dynamic.partitions=1000;
set hive.exec.max.dynamic.partitions.pernode=1000;
----------------------------------------------
INSERT OVERWRITE TABLE bi_corp_common.stk_cys_inhabilitadosrisk
PARTITION(partition_date)
SELECT CAST(INH.nro_inh AS BIGINT) cod_cys_inh
	, TRIM(INH.tpo_pers) cod_per_tipopersona 
	, TRIM(INH.ape_pers) ds_per_apellido
	, TRIM(INH.nom_pers) ds_per_nombre
	, TRIM(INH.cod_sex_pers) cod_per_sexo
	, TRIM(INH.fec_naci) dt_per_fechanacimiento
	, TRIM(DS.ds_tipodoc) ds_per_tipodoc
	, CAST(INH.nro_doc AS BIGINT) ds_per_numdoc
	, TRIM(INH.cod_caus) cod_cys_causal
	, TRIM(CA.ds_caus) ds_cys_causal
	, TRIM(INH.sec_caus) int_cys_seccaus
	, TRIM(INH.fec_inh) dt_cys_inh
	, TRIM(INH.fec_rehb) dt_cys_rehb
	, IF(TRIM(INH.vigencia) = 'S', 1, 0) flag_cys_vigencia
	, last_day(to_date(INH.fec_inh)) partition_date
FROM bi_corp_staging.afir_in00 INH 
LEFT JOIN 
	(SELECT DISTINCT TRIM(substr(gen_clave, 1, 1)) tipodoc, gen_datos ds_tipodoc
	 FROM bi_corp_staging.tcdtgen WHERE gentabla = '0113') DS
	ON TRIM(INH.tpo_doc) = DS.tipodoc 
LEFT JOIN bi_corp_staging.afir_cod_causales CA 
	ON TRIM(INH.cod_caus) = TRIM(CA.cod_caus)
WHERE INH.partition_date = '2021-01-26' 
AND TRIM(INH.cod_caus) IN ('017','018','019') ;



SHOW PARTITIONS bi_corp_common.stk_cys_inhabilitadosrisk ;



------------------------------------------------------------------------------------




-- DROP TABLE bi_corp_common.stk_tcr_normalizaciontarjetas ;
CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_common.stk_tcr_normalizaciontarjetas (
	cod_per_nup	bigint ,
	cod_tcr_entidad	string ,
	cod_suc_sucursal	int ,
	cod_nro_cuenta	bigint ,
	cod_prod_producto	string ,
	cod_prod_subproducto	string ,
	cod_div_divisa	string ,
	int_tcr_secuencia	int ,
	dt_tcr_fechacuotaphone	string ,
	cod_tcr_cuentabase	bigint ,
	cod_tcr_marcainicial	string ,
	cod_tcr_submarcainicial	string ,
	cod_tcr_marcasegini	string ,
	cod_tcr_tiporeestructuracionini	string ,
	cod_tcr_marcaactual	string ,
	cod_tcr_submarcaactual	string ,
	dt_tcr_fechacambioseg	string ,
	cod_tcr_marcasegact	string ,
	cod_tcr_tiporeestructuracionact	string ,
	dt_tcr_fechacura	string ,
	dt_tcr_fechaempeora	string ,
	dt_tcr_fechanormaliza	string ,
	ds_tcr_motivocambio	string ,
	int_tcr_ree	int ,
	dt_tcr_fechacarga	string )
PARTITIONED BY(partition_date string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '/santander/bi-corp/common/cys/stk_tcr_normalizaciontarjetas' ;


set hive.execution.engine=spark;
set mapred.job.queue.name=root.dataeng;
set hive.exec.dynamic.partition.mode=nonstrict;
-----------------------------------------------
INSERT OVERWRITE TABLE bi_corp_common.stk_tcr_normalizaciontarjetas
PARTITION(partition_date)
SELECT CAST(gitccuo_num_persona AS BIGINT) cod_per_nup 	 
	, TRIM(gitccuo_cod_entigen) cod_tcr_entidad 	
	, CAST(gitccuo_cod_centro AS INT) cod_suc_sucursal 	 
	, CAST(gitccuo_num_contrato AS BIGINT) cod_tcr_cuenta 	
	, TRIM(gitccuo_cod_producto) cod_tcr_producto 	
	, TRIM(gitccuo_cod_subprodu) cod_tcr_subproducto 	
	, TRIM(gitccuo_cod_divisa) cod_tcr_divisa 	
	, CAST(gitccuo_num_secuencia AS INT)  int_tcr_secuencia 	 
	, TRIM(gitccuo_fec_cuotaphone) dt_tcr_cuotaphone 	 
	, CAST(gitccuo_cuenta_visa AS BIGINT) cod_tcr_cuentabase 	
	, TRIM(gitccuo_cod_marca_ini) cod_tcr_marcainicial 	
	, TRIM(gitccuo_cod_submarca_ini) cod_tcr_submarcainicial 	
	, TRIM(gitccuo_cod_marca_seg_ini) cod_tcr_marcasegini 	
	, TRIM(gitccuo_tip_reestruct_ini) cod_tcr_tiporeestructuracionini 	
	, TRIM(gitccuo_cod_marca_act) cod_tcr_marcaactual 	
	, TRIM(gitccuo_cod_submarca_act) cod_tcr_submarcaactual 	
	, TRIM(gitccuo_fec_cambio_seg) dt_tcr_cambioseg 	 
	, TRIM(gitccuo_cod_marca_seg_act) cod_tcr_marcasegact 	 
	, TRIM(gitccuo_tip_reestruct_act) cod_tcr_tiporeestructuracionact 	
	, TRIM(gitccuo_fec_cura) dt_tcr_cura 	 
	, IF(TRIM(gitccuo_fec_empeora) = '9999-12-31', NULL, TRIM(gitccuo_fec_empeora)) dt_tcr_empeora 	 
	, IF(TRIM(gitccuo_fec_normaliza) = '9999-12-31', NULL, TRIM(gitccuo_fec_normaliza)) dt_tcr_normaliza 	 
	, TRIM(gitccuo_motivo_cambio) ds_tcr_motivocambio 	
	, CAST(gitccuo_num_ree AS INT) int_tcr_ree 	 
	, SUBSTRING(gitccuo_timest_umo, 1, 10) dt_tcr_carga 	 
	, partition_date 	
FROM bi_corp_staging.garra_log_cuotaphone ;

SELECT * FROM bi_corp_common.stk_tcr_normalizaciontarjetas ;


------------------------------------------------------------------------
-- DROP TABLE bi_corp_common.stk_rcp_contratosventamoria ;
CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_common.stk_rcp_contratosventamoria (
  	cod_rcp_venta int,
	cod_suc_sucursal int, --
	cod_nro_cuenta string,
	cod_pro_producto int,
	cod_pro_subproducto string,
	cod_div_divisa string,
	cod_per_nup bigint,
	cod_rcp_tipodocumento string,
	ds_rcp_documento bigint,
	ds_rcp_penomfan int,
	ds_rcp_apellidocliente int,
	ds_rcp_nombrecliente string,
	cod_rcp_tipodocumentoi string,
	ds_rcp_documentoi bigint,
	ds_rcp_apellidoclientei string,
	ds_rcp_nombreclientei string,
	fc_rcp_totalcapital decimal(17,4),
	fc_rcp_totalinteres decimal(17,4),
	fc_rcp_totalcomision decimal(17,4),
	fc_rcp_totalgastos decimal(17,4),
	fc_rcp_totalseguros decimal(17,4),
	fc_rcp_totalimpuesto decimal(17,4),
	fc_rcp_totalajuste decimal(17,4),
	fc_rcp_saldo decimal(17,4),
	fc_rcp_saldoinformadoi decimal(17,4),
	fc_rcp_importerecupera decimal(17,4),
	fc_rcp_imptotcanco decimal(17,4),
	fc_rcp_impcancgasc decimal(17,4),
	fc_rcp_totalcontable decimal(17,4),
	fc_rcp_totalnocontable decimal(17,4),
	fc_rcp_totalimpuperc decimal(17,4),
	fc_rcp_cantidadpagos int,
	cod_rcp_estado string,
	dt_rcp_fechacastigo string,
	cod_rcp_origencast string,
	dt_rcp_fechaabseguimiento string,
	dt_rcp_fechaabtotal string,
	dt_rcp_fechabaja string,
	cod_rcp_motivobaja string,
	dt_rcp_fechainisituirregular string,
	cod_rcp_coefinde string,
	cod_rcp_refinanciacion string,
	flag_rcp_conversi int,
	cod_rcp_tipadmin string,
	cod_rcp_procedencia string,
	ds_rcp_nombreeejj string,
	dt_rcp_fechaalta string,
	ds_rcp_usuarioalta string,
	ds_rc_usuarioumo string,
	ts_rcp_umo timestamp,
	cod_rcp_productoemerix string,
	cod_rcp_tipocartera string,
	cod_rcp_escenario string,
	cod_rcp_bufette int,
	ds_rcp_segmentocomercial string,
	flag_rcp_rechazo int,
	cod_rcp_rechazo string,
	ds_rcp_motivorechazo string )
PARTITIONED BY(partition_date string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '/santander/bi-corp/common/recuperaciones/stk_rcp_contratosventamoria' ;


SET hive.merge.mapfiles=true;
SET hive.merge.mapredfiles=true;
SET hive.merge.size.per.task=4000000;
SET hive.merge.smallfiles.avgsize=16000000;
SET hive.exec.dynamic.partition.mode=nonstrict;
SET mapred.job.queue.name=root.dataeng;

INSERT OVERWRITE TABLE bi_corp_common.stk_rcp_contratosventamoria 
PARTITION(partition_date) 

SELECT 
	CAST(idventa AS INT) idventa ,
	CAST(cod_centro AS INT) cod_centro ,
	num_contrato ,
	CAST(cod_producto AS INT) cod_producto ,
	TRIM(cod_subprodu) cod_subprodu ,
	TRIM(cod_divisa) cod_divisa ,
	CAST(penumper AS BIGINT) penumper ,
	TRIM(petipdoc) petipdoc ,
	CAST(penumdoc AS BIGINT) penumdoc ,
	IF(TRIM(penomfan) = '', NULL, TRIM(penomfan)) penomfan ,
	TRIM(pepriape) pepriape ,
	TRIM(penomper) penomper ,
	TRIM(petipdoc_i) petipdoc_i ,
	CAST(penumdoc_i AS BIGINT) penumdoc_i ,
	TRIM(pepriape_i) pepriape_i ,
	TRIM(penomper_i) penomper_i ,
	CAST(tot_capital AS INT) tot_capital ,
	CAST(tot_interes AS INT) tot_interes ,
	CAST(tot_comision AS INT) tot_comision ,
	CAST(tot_gastos AS INT) tot_gastos ,
	CAST(tot_seguros AS INT) tot_seguros ,
	CAST(tot_impuesto AS INT) tot_impuesto ,
	CAST(tot_ajuste AS INT) tot_ajuste ,
	CAST(saldo AS INT) saldo ,
	CAST(saldo_informado_i AS INT) saldo_informado_i ,
	CAST(imp_recuperar AS INT) imp_recuperar ,
	CAST(imp_totcanco AS INT) imp_totcanco ,
	CAST(imp_cancgasc AS INT) imp_cancgasc ,
	CAST(total_contable AS INT) total_contable ,
	CAST(total_no_contable AS INT) total_no_contable ,
	CAST(total_impuperc AS INT) total_impuperc ,
	CAST(cant_pagos AS INT) cant_pagos ,
	TRIM(cod_estado) cod_estado ,
	IF(TRIM(fec_castigo) IN ('9999-12-31','0001-01-01'), NULL, TRIM(fec_castigo) ) fec_castigo , 
	IF(TRIM(cod_origcast) = '', NULL, TRIM(cod_origcast)) cod_origcast ,
	IF(TRIM(fec_abseguim) IN ('9999-12-31','0001-01-01'), NULL, fec_abseguim ) fec_abseguim , 
	IF(TRIM(fec_abtotal) IN ('9999-12-31','0001-01-01'), NULL, fec_abtotal ) fec_abtotal , 
	IF(TRIM(fec_baja) IN ('9999-12-31','0001-01-01'), NULL, fec_baja ) fec_baja , 
	TRIM(cod_motbajac) cod_motbajac ,
	IF(TRIM(fec_inisitir) IN ('9999-12-31','0001-01-01'), NULL, fec_inisitir ) fec_inisitir ,
	IF(TRIM(cod_coefinde) = '', NULL, TRIM(cod_coefinde)) cod_coefinde ,
	IF(TRIM(cod_refinanc) = '', NULL, TRIM(cod_refinanc)) cod_refinanc ,
	IF(TRIM(ind_conversi) = 'S', 1, 0) ind_conversi ,
	TRIM(cod_tipadmin) cod_tipadmin ,
	IF(TRIM(cod_proceden) = '', NULL, TRIM(cod_proceden)) cod_proceden ,
	TRIM(nombre_eejj_i) nombre_eejj_i ,
	fecha_alta ,
	IF(TRIM(userid_alta) = '', NULL, TRIM(userid_alta)) userid_alta ,
	IF(TRIM(userid_umo) = '', NULL, TRIM(userid_umo)) userid_umo ,
	from_unixtime(unix_timestamp(timest_umo ,'dd-MM-yyyy HH:mm:ss'),'yyyy-MM-dd  HH:mm:ss') timest_umo ,
	TRIM(prod_emerix) prod_emerix ,
	TRIM(tip_cartera) tip_cartera ,
	TRIM(escenario) escenario ,
	CAST(nup_eejj AS INT) nup_eejj ,
	TRIM(segmento_comer) segmento_comer ,
	IF(TRIM(ind_rechazo) = 'S', 1, 0) ind_rechazo ,
	TRIM(cod_rechazo) cod_rechazo ,
	IF(TRIM(motivo_rechazo) = '', NULL, TRIM(motivo_rechazo)) motivo_rechazo ,
	partition_date
FROM bi_corp_staging.moria_vc_historico_ventas ;



SELECT * FROM bi_corp_common.stk_rcp_contratosventamoria  ;

-- DROP TABLE bi_corp_common.stk_rcp_ventamoria ;
CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_common.stk_rcp_ventamoria (
  cod_rcp_venta int,
  cod_rcp_fedeicomiso int,
  cod_rcp_preventa int,
  cod_rcp_cotizacion int,
  dt_rcp_fechacorte string,
  cod_rcp_ip string,
  cod_rcp_usuario string,
  ds_rcp_estado string,
  dt_rcp_fechapreventa string,
  fc_rcp_cantcliagregados int,
  fc_rcp_cantcliexcluidos int,
  dt_rcp_fechaventa string,
  ds_rcp_usuarioalta string,
  ts_rcp_alta timestamp,
  ds_rcp_usuarioenvio string,
  ts_rcp_envio timestamp,
  ds_rcp_usuarioaprobacion string,
  ts_rcp_aprobacion timestamp,
  ds_rcp_motivorechazo string,
  dt_rcp_fechaajuste string,
  dt_rcp_fechaajuste2 string )
PARTITIONED BY(partition_date string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '/santander/bi-corp/common/recuperaciones/stk_rcp_ventamoria' ;



SET hive.merge.mapfiles=true;
SET hive.merge.mapredfiles=true;
SET hive.merge.size.per.task=4000000;
SET hive.merge.smallfiles.avgsize=16000000;
SET hive.exec.dynamic.partition.mode=nonstrict;
SET mapred.job.queue.name=root.dataeng;

INSERT OVERWRITE TABLE bi_corp_common.stk_rcp_ventamoria 
PARTITION(partition_date) 

SELECT 
	CAST(idventa AS INT) idventa ,
	CAST(idfideicomiso AS INT) idfideicomiso ,
	CAST(idpreventa AS INT) idpreventa ,
	CAST(cotizacion AS INT) cotizacion ,
	TRIM(fecha_corte) fecha_corte ,
	IF(TRIM(ip) = '', NULL, TRIM(ip)) ip ,
	IF(TRIM(usuario) = '', NULL, TRIM(usuario)) usuario ,
	TRIM(estado) estado ,
	TRIM(fecha_preventa) fecha_preventa ,
	TRIM(clientes_agregados) clientes_agregados ,
	TRIM(clientes_excluidos) clientes_excluidos ,
	TRIM(fecha_venta) fecha_venta ,
	TRIM(usuario_alta) usuario_alta ,
	from_unixtime(unix_timestamp(timest_alta ,'dd-MM-yyyy HH:mm:ss'),'yyyy-MM-dd  HH:mm:ss') timest_alta , 
	TRIM(usuario_envio) usuario_envio ,
	from_unixtime(unix_timestamp(timest_envio ,'dd-MM-yyyy HH:mm:ss'),'yyyy-MM-dd  HH:mm:ss') timest_envio , 
	TRIM(usuario_aprobacion) usuario_aprobacion ,
	from_unixtime(unix_timestamp(timest_aprobacion ,'dd-MM-yyyy HH:mm:ss'),'yyyy-MM-dd  HH:mm:ss') timest_aprobacion , 
	IF(TRIM(motivo_rechazo) = '', NULL, TRIM(motivo_rechazo)) motivo_rechazo ,
	IF(TRIM(fec_ajuste1) = '', NULL, TRIM(fec_ajuste1)) fec_ajuste1 ,
	IF(TRIM(fec_ajuste2) = '', NULL, TRIM(fec_ajuste2)) fec_ajuste2 ,
	partition_date
FROM bi_corp_staging.moria_vc_ventas ;


SELECT * FROM bi_corp_common.stk_rcp_ventamoria ;

SHOW PARTITIONS bi_corp_staging.garra_wagucdex ;

DESCRIBE bi_corp_staging.garra_wagucdex ;


			SELECT
				waguxdex_num_persona penumper ,
				waguxdex_cod_producto pecodpro ,
				waguxdex_cod_subprodu pecodsub ,
				partition_date ,
				CAST (waguxdex_num_contrato AS bigint) penumcon , --cast(waguxdex_num_contrato as bigint)
				pecodofi sucursal , --cast(waguxdex_cod_centro as int)
				waguxdex_fecha_altareg pefecalt ,
				waguxdex_fec_incumplim pefecbrb
			FROM
				bi_corp_staging.garra_wagucdex -- bi_corp_staging.malpe_pedt008
			WHERE
				partition_date = '2020-08-09' ;
				-- pecalpar = 'TI' 



DESCRIBE bi_corp_staging.malpe_pedt008 ;







---------------------------------------------------------------------------------


-- Les comparto la lógica para poblar "por default" todos los registros de la tabla de Activo para el campo idRU:

-- 1) Con el NUP del Cliente (que es el idf_pers_ods sin el 10072) y el cod_segmento_gest
-- acceder a bi_corp_staging.malpe_pedt030 (tomar la última partición del mes sobre el
-- que se está trabajando; si proceso Diciembre, tomar 2020-12-30) y obtener el pesegcal.

-- DESCRIBE bi_corp_staging.malpe_pedt030 ;   
-- SHOW PARTITIONS bi_corp_staging.malpe_pedt030 ;
   
SELECT 	DISTINCT penumper 
	, pesegcal
FROM bi_corp_staging.malpe_pedt030
WHERE 	partition_date = '2020-12-30' ;

-- 2) Por otro lado, con el cod_producto y cod_subprodu_altair (que hay que ver en la descarga
-- del MIS cómo obtenerlos; en el query debajo figuran como a.), acceder a las siguientes tablas:

   LEFT JOIN bi_corp_bdr.v_baremos_local vbl
                 ON TRIM(vbl.cod_baremo_alfanumerico_local) = TRIM(concat(a.cod_producto, a.cod_subprodu_altair))
                AND vbl.cod_negocio_local = '3'
   LEFT JOIN bi_corp_bdr.v_map_baremos_global_local vblg
                 ON vblg.cod_baremo_local = vbl.cod_baremo_local
                AND vblg.cod_negocio = 3
               
-- Y recuperar el campo vblg.cod_baremo_global.


                
                
WITH rs_activo AS (

	SELECT DISTINCT regexp_replace(RE.cod_ren_pers_ods, '10072', '') penumper
		, RE.cod_producto_operacional cod_producto
		, RE.cod_ren_subprodu cod_subprodu
	FROM bi_corp_common.bt_ror_input_activo RE
	WHERE RE.partition_date = '2020-12-31' 
	AND RE.cod_ren_pers_ods != '-99100' )
	
, rs_segmento AS (
	
	SELECT RE.penumper
		, RE.cod_producto
		, RE.cod_subprodu
		, SE.pesegcal
	FROM rs_activo RE
	LEFT JOIN 
		(SELECT DISTINCT p30.penumper 
			, BA.cod_baremo_local pesegcal
		FROM bi_corp_staging.malpe_pedt030 p30
		LEFT JOIN bi_corp_staging.param_baremo_local BA
		ON TRIM(BA.cod_baremo_alfanumerico_local) = TRIM(p30.pesegcal)
		WHERE 	partition_date = '2020-12-30' ) SE
		ON RE.penumper = SE.penumper
	)
	
SELECT * FROM rs_segmento WHERE pesegcal IS NOT NULL ;



SELECT * FROM bi_corp_common.dim_ren_areanegocioctr WHERE cod_ren_area_negocio = 'AN'

select DISTINCT cod_segmento_gest from bi_corp_staging.rio157_ms0_ft_dwh_blce_result
WHERE partition_date = '2020-12-31'



SELECT nro_doc 
FROM bi_corp_staging.afir_in00 WHERE partition_date = '2021-02-08'
GROUP BY nro_doc HAVING COUNT(nro_doc) > 1 ;

SELECT * FROM bi_corp_staging.afir_in00 
WHERE partition_date = '2021-02-08'
AND nro_inh = '2031150' ;


SELECT * FROM bi_corp_common.dim_tie_calendario 
WHERE partition_date = '2021-02-08' AND dt_tie_date = '20'

SHOW PARTITIONS bi_corp_staging.rio125_adsf_cartera



-- 0000
-- 1001
-- 1002
-- 2001
-- null
-- 2005
-- 2006
SELECT DISTINCT cod_subprodu , SUBSTRING(idf_cto_ods ,11,4) sub_producalc , cod_contenido 
FROM bi_corp_staging.rio157_ms0_dt_dwh_generic_result 
WHERE partition_date = '2021-01-31'
AND cod_subprodu != SUBSTRING(idf_cto_ods ,11,4)
-- AND cod_subprodu = '1001'
AND cod_contenido = 'COM'


SHOW PARTITIONS bi_corp_common.bt_ren_result ;


SELECT DISTINCT cod_baremo_alfanumerico_local, cod_baremo_local
FROM bi_corp_bdr.baremos_local bl24
WHERE bl24.cod_negocio_local = '24' ;



SELECT * FROM bi_corp_staging.rio187_gestiones 
WHERE partition_date = '2021-02-11' ;


SELECT DISTINCT 
	*
FROM bi_corp_bdr.map_baremos_global_local mb9
WHERE mb9.cod_negocio = 24 ;


	
SELECT * FROM bi_corp_staging.rio187_gestiones WHERE partition_date = '2021-02-11' ;

SHOW PARTITIONS bi_corp_staging.rio187_gestiones ;



DROP TABLE bi_corp_ic.tb_clientes_dia ;

MSCK REPAIR TABLE bi_corp_ic.tb_clientes_dia ;


SELECT * FROM bi_corp_ic.tb_clientes_dia WHERE partition_date = '2021-02-14'



SELECT * FROM bi_corp_staging.alha9601 ;


SHOW PARTITIONS bi_corp_ic.tb_clientes_dia ;

SELECT * FROM bi_corp_ic.tb_clientes_dia WHERE partition_date = '2021-02-16'


set mapred.job.queue.name=root.dataeng;
set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=4000000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.auto.convert.join=true;
set hive.auto.convert.join.noconditionaltask=true;
set hive.auto.convert.join.noconditionaltask.size=40000000;

DROP TABLE IF EXISTS temp_saldos_depositos_prestamos;
CREATE TEMPORARY TABLE temp_saldos_depositos_prestamos
AS
SELECT
    RD.partition_date AS partition_date,
    RD.cod_ren_pais AS COD_REN_PAIS,
    RD.cod_ren_divisa AS COD_DIVISA,
    RD.cod_ren_area_negocio AREA_NEGOCIO,
    RD.cod_ren_cont_gestion AS COD_CTA_CONT_GESTION,
    RD.cod_ren_entidad_espana AS COD_ENTIDAD_ESPANA,
    RD.dt_ren_altacontrato AS dt_ren_altacontrato,
    RD.dt_ren_vencontrato AS dt_ren_vencontrato,
    RD.fc_ren_tasint as fc_ren_tasint,
    RD.fc_ren_plazcontractual as fc_ren_plazcontractual,
    IF(RD.fc_ren_tasint=0,1,0) as flag_ren_tasa0,
    RD.flag_ren_ind_pool as flag_ren_ind_pool,
   	RD.cod_ren_segmento_gest as cod_ren_segmento_gest,
    RD.cod_ren_producto_gest AS COD_PRODUCTO_GEST,
    RD.ds_ren_producto_niv_3 AS ds_ren_producto_niv_3,
    RD.ds_ren_producto_niv_4 AS ds_ren_producto_niv_4,
    RD.ds_ren_producto_niv_5 AS ds_ren_producto_niv_5,
    RD.ds_ren_producto_niv_6 AS ds_ren_producto_niv_6,
    RD.ds_ren_producto_niv_7 AS ds_ren_producto_niv_7,
    RD.ds_ren_producto_niv_8 AS ds_ren_producto_niv_8,
    RD.cod_ren_producto_niv_3 AS cod_ren_producto_niv_3,
    RD.cod_ren_producto_niv_4 AS cod_ren_producto_niv_4,
    RD.cod_ren_producto_niv_5 AS cod_ren_producto_niv_5,
    RD.cod_ren_producto_niv_6 AS cod_ren_producto_niv_6,
    RD.cod_ren_producto_niv_7 AS cod_ren_producto_niv_7,
    RD.cod_ren_producto_niv_8 AS cod_ren_producto_niv_8,
    RD.cod_ren_area_negocio_niv_2 AS cod_ren_area_negocio_niv_2,
    RD.cod_ren_area_negocio_niv_3 AS cod_ren_area_negocio_niv_3,
    RD.ds_ren_area_negocio_niv_2 AS ds_ren_area_negocio_niv_2,
    RD.ds_ren_area_negocio_niv_3 AS ds_ren_area_negocio_niv_3,
    RD.cod_ren_balance_niv_7 AS cod_ren_balance_niv_7,
    RD.ds_ren_balance_niv_7 AS ds_ren_balance_niv_7,
    sum(RD.fc_ren_saldo_puntual_ml) as fc_ren_saldo_puntual_ml,
    sum(RD.fc_ren_saldo_puntual_mo) as fc_ren_saldo_puntual_mo,
    sum(RD.fc_ren_saldo_medio_ml) as fc_ren_saldo_medio_ml,
    sum(RD.fc_ren_saldo_medio_mo) as fc_ren_saldo_medio_mo,
    sum(RD.fc_ren_saldo_puntual_ml) * (RD.fc_ren_tasint / 100) as fc_ren_saldo_ponderado_ml,
    sum(RD.fc_ren_saldo_puntual_mo) * (RD.fc_ren_tasint / 100) as fc_ren_saldo_ponderado_mo,
    count(DISTINCT RD.cod_ren_contrato) as fc_ren_contratos
FROM
    bi_corp_common.bt_ren_result_dia AS RD
WHERE
--- Completar Key y Dag_id
  RD.partition_date = '2021-02-08'
  AND
    (
    RD.cod_ren_producto_niv_4 IN ('BG120','BG215')
    OR RD.cod_ren_producto_gest IN ( 'AC01IC132', 'AC01BA007', 'AC01CA001', 'PA01CC022', 'PA01CC023', 'PA02CC004', 'AC01CA005' )
        )
GROUP BY
   RD.partition_date,
    RD.cod_ren_pais,
    RD.cod_ren_divisa,
    RD.cod_ren_area_negocio,
    RD.cod_ren_cont_gestion,
    RD.cod_ren_entidad_espana,
    RD.dt_ren_altacontrato,
    RD.dt_ren_vencontrato,
    RD.fc_ren_tasint,
    RD.fc_ren_plazcontractual,
    IF(RD.fc_ren_tasint=0,1,0),
    RD.flag_ren_ind_pool,
    RD.cod_ren_segmento_gest,
    RD.cod_ren_producto_gest,
    RD.ds_ren_producto_niv_3,
    RD.ds_ren_producto_niv_4,
    RD.ds_ren_producto_niv_5,
    RD.ds_ren_producto_niv_6,
    RD.ds_ren_producto_niv_7,
    RD.ds_ren_producto_niv_8,
    RD.cod_ren_producto_niv_3,
    RD.cod_ren_producto_niv_4,
    RD.cod_ren_producto_niv_5,
    RD.cod_ren_producto_niv_6,
    RD.cod_ren_producto_niv_7,
    RD.cod_ren_producto_niv_8,
    RD.cod_ren_area_negocio_niv_2,
    RD.cod_ren_area_negocio_niv_3,
    RD.ds_ren_area_negocio_niv_2,
    RD.ds_ren_area_negocio_niv_3,
    RD.cod_ren_balance_niv_7,
    RD.ds_ren_balance_niv_7;

SET hive.exec.dynamic.partition.mode=nonstrict;

INSERT OVERWRITE TABLE bi_corp_business.agg_ren_prestamos_depositos_daily
PARTITION(partition_date)
SELECT
    RD.COD_DIVISA AS cod_ren_divisa,
    RD.COD_CTA_CONT_GESTION AS cod_ren_cta_cont_gestion,
    RD.COD_ENTIDAD_ESPANA AS cod_ren_entidad_espana,
    RD.dt_ren_altacontrato AS dt_ren_altacontrato,
    RD.dt_ren_vencontrato AS dt_ren_vencontrato,
    RD.fc_ren_tasint as fc_ren_tasint,
    RD.fc_ren_plazcontractual as fc_ren_plazcontractual,
    RD.flag_ren_tasa0 as flag_ren_tasa0,
    RD.flag_ren_ind_pool as flag_ren_ind_pool,
    RD.cod_ren_area_negocio_niv_2 AS cod_ren_area_negocio_niv_2,
    RD.cod_ren_area_negocio_niv_3 AS cod_ren_area_negocio_niv_3,
    RD.ds_ren_area_negocio_niv_2 AS ds_ren_area_negocio_niv_2,
    RD.ds_ren_area_negocio_niv_3 AS ds_ren_area_negocio_niv_3,
    RD.ds_ren_producto_niv_3 AS ds_ren_producto_niv_3,
    RD.ds_ren_producto_niv_4 AS ds_ren_producto_niv_4,
    RD.ds_ren_producto_niv_5 AS ds_ren_producto_niv_5,
    RD.ds_ren_producto_niv_6 AS ds_ren_producto_niv_6,
    RD.ds_ren_producto_niv_7 AS ds_ren_producto_niv_7,
    RD.ds_ren_producto_niv_8 AS ds_ren_producto_niv_8,
    RD.cod_ren_producto_niv_3 AS cod_ren_producto_niv_3,
    RD.cod_ren_producto_niv_4 AS cod_ren_producto_niv_4,
    RD.cod_ren_producto_niv_5 AS cod_ren_producto_niv_5,
    RD.cod_ren_producto_niv_6 AS cod_ren_producto_niv_6,
    RD.cod_ren_producto_niv_7 AS cod_ren_producto_niv_7,
    RD.cod_ren_producto_niv_8 AS cod_ren_producto_niv_8,
    RD.cod_ren_balance_niv_7 AS cod_ren_balance_niv_7,
    RD.ds_ren_balance_niv_7 AS ds_ren_balance_niv_7,
    seg.banca as ds_ren_banca,
    seg.segmento_nivel_1 as ds_ren_segmento_nivel_1,
    seg.segmento_nivel_2 as ds_ren_segmento_nivel_2,
    sum(RD.fc_ren_saldo_puntual_ml) as fc_ren_saldo_puntual_ml,
    sum(RD.fc_ren_saldo_puntual_mo) as fc_ren_saldo_puntual_mo,
	sum(RD.fc_ren_saldo_medio_ml) as fc_ren_saldo_medio_ml,
	sum(RD.fc_ren_saldo_medio_mo) as fc_ren_saldo_medio_mo,
	sum(RD.fc_ren_saldo_ponderado_ml) as fc_ren_saldo_ponderado_ml,
	sum(RD.fc_ren_saldo_ponderado_mo) as fc_ren_saldo_ponderado_mo,
	SUM(RD.fc_ren_contratos) as fc_ren_contratos,
    RD.partition_date
FROM
    temp_saldos_depositos_prestamos RD
LEFT JOIN bi_corp_cg.segmento_cdg seg on substr(rd.cod_ren_segmento_gest,1,1)=seg.cod_seg
GROUP BY
    RD.COD_DIVISA,
    RD.COD_CTA_CONT_GESTION,
    RD.COD_ENTIDAD_ESPANA,
    RD.dt_ren_altacontrato,
    RD.dt_ren_vencontrato,
    RD.fc_ren_tasint,
    RD.fc_ren_plazcontractual,
    RD.flag_ren_tasa0,
    RD.flag_ren_ind_pool,
    RD.cod_ren_area_negocio_niv_2,
    RD.cod_ren_area_negocio_niv_3,
    RD.ds_ren_area_negocio_niv_2,
    RD.ds_ren_area_negocio_niv_3,
    RD.ds_ren_producto_niv_3,
    RD.ds_ren_producto_niv_4,
    RD.ds_ren_producto_niv_5,
    RD.ds_ren_producto_niv_6,
    RD.ds_ren_producto_niv_7,
    RD.ds_ren_producto_niv_8,
    RD.cod_ren_producto_niv_3,
    RD.cod_ren_producto_niv_4,
    RD.cod_ren_producto_niv_5,
    RD.cod_ren_producto_niv_6,
    RD.cod_ren_producto_niv_7,
    RD.cod_ren_producto_niv_8,
	RD.cod_ren_balance_niv_7,
    RD.ds_ren_balance_niv_7,
    seg.banca,
    seg.segmento_nivel_1,
	seg.segmento_nivel_2,
    RD.partition_date;
   
   
   

SELECT * FROM bi_corp_ic.tb_clientes_dia2 WHERE partition_date = '2021-02-17' ;



set hive.execution.engine=spark;
set mapred.job.queue.name=root.dataeng;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions=1800;
set hive.exec.max.dynamic.partitions.pernode=1800;
INSERT overwrite TABLE bi_corp_common.bt_suc_turnero PARTITION (partition_date)
SELECT CAST(ET.id_turno AS BIGINT) cod_suc_turno
	, CAST(ET.id_estado AS INT) cod_suc_estado
	, TRIM(EE.descri) ds_suc_estado
	, DS.ds_tipodoc ds_suc_tipodoc
	, IF(TRIM(ET.nrodoc) = 'null', NULL, CAST(ET.nrodoc AS BIGINT)) ds_per_numdoc
	, IF(TRIM(ET.sector) = 'P', 'PLATAFORMA', IF(TRIM(ET.sector) = 'C', 'CAJA', NULL)) ds_suc_sector
	, CAST(ET.id_suc AS INT) cod_suc_sucursal
	, TRIM(SUC.descri) ds_suc_sucursal
	, ET.fecha_hora_desde
	, ET.fecha_hora_hasta
	, TRIM(ET.fraccion) ds_suc_fraccion
	, TRIM(ET.dia) ds_suc_dia
	, IF(TRIM(ET.nup) = 'null', NULL, CAST(ET.nup AS BIGINT)) cod_per_nup  
	, IF(TRIM(ET.apellido) = 'null', NULL, TRIM(ET.apellido)) ds_per_apellido
	, IF(TRIM(ET.nombre) = 'null', NULL, TRIM(ET.nombre)) ds_per_nombre
	, IF(TRIM(ET.pesubseg) = 'null', NULL, TRIM(ET.pesubseg)) cod_suc_subseg  
	, IF(TRIM(ET.pesexper) = 'null', NULL, TRIM(ET.pesexper)) cod_per_sexo
	, CAST(ET.id_motivo AS INT) cod_suc_motivovisita
	, TRIM(MV.descripcion) ds_suc_motivovisita
	, IF(TRIM(ET.mail) = 'null', NULL, TRIM(ET.mail)) ds_suc_mail
	, IF(TRIM(ET.cuit) = 'null', NULL, CAST(ET.cuit AS BIGINT)) ds_per_cuit
	, IF(TRIM(ET.id_turno_orig) = 'null', NULL, CAST(ET.id_turno_orig AS INT)) cod_suc_turnooriginal
	, IF(TRIM(ET.id_ejec_atencion) = 'null', NULL, TRIM(ET.id_ejec_atencion)) cod_suc_ejecatencion
	, IF(TRIM(ET.id_suc_atencion) = 'null', NULL, TRIM(ET.id_suc_atencion)) cod_suc_sucursalatencion
	, TRIM(ET.id_puesto_citas) cod_suc_puestocitas
	, IF(TRIM(ET.telefono) = 'null', NULL, TRIM(ET.telefono)) ds_suc_telefono
	, TRIM(ET.id_tipo_turno) cod_suc_tipoturno
	, IF(TRIM(ET.id_tipo_turno) = '1', 'FISICO', IF(TRIM(ET.id_tipo_turno) = '2', 'REMOTO', NULL)) ds_suc_tipoturno
	, SUBSTRING(ET.fecha_desde, 1, 19) ts_suc_fechadesde
	, SUBSTRING(ET.fecha_hasta, 1, 19) ts_suc_fechahasta
	, ET.partition_date
FROM bi_corp_staging.rio154_enc_turno ET 
LEFT JOIN bi_corp_staging.rio154_enc_estado EE 
	ON ET.id_estado = EE.id_estado
	AND ET.partition_date = EE.partition_date 
LEFT JOIN bi_corp_staging.rio154_motivo_visita_v2 MV 
	ON ET.id_motivo = MV.id_motivo
	AND ET.partition_date = MV.partition_date 
LEFT JOIN bi_corp_staging.rio154_sucursalrio SUC 
	ON ET.id_suc = SUC.codigo 
	AND ET.partition_date = SUC.partition_date 
LEFT JOIN 
	(SELECT DISTINCT TRIM(substr(gen_clave, 1, 1)) tipodoc, gen_datos ds_tipodoc
	 FROM bi_corp_staging.tcdtgen WHERE gentabla = '0113') DS
	ON TRIM(ET.tipodoc) = DS.tipodoc
WHERE SUBSTRING(ET.partition_date, 1, 4) = '2019';

show partitions bi_corp_common.bt_suc_turnero ;


show partitions bi_corp_staging.rio154_enc_turno ;


INVALIDATE METADATA bi_corp_common.bt_suc_turnero ;
REFRESH bi_corp_common.bt_suc_turnero ;





SELECT * FROM bi_corp_staging.nesr_encolador_d WHERE partition_date='2021-02-26'

SHOW PARTITIONS bi_corp_staging.nesr_encolador_d

SHOW PARTITIONS bi_corp_staging.acal_tcalif_persona ;


SELECT * 

FROM bi_corp_staging.acal_tcalif_persona WHERE partition_date = '2021-03-05' ;





