


   DROP TABLE interaccion_repartition_tmp ;
   CREATE TEMPORARY TABLE interaccion_repartition_tmp AS
   WITH interaccion_repartition AS (
   
	   SELECT cd_interaccion 
	   		, nup 
	   		, cd_ejecutivo 
	   		, dt_inicio 
	   		, dt_cierre 
	   		, cd_canal_comunicacion 
	   		, cd_canal_venta 
	   		, cd_sucursal 
	   		, ds_comentario 
	   		, to_date(dt_inicio) p_date
	   FROM bi_corp_staging.rio151_tbl_interaccion
	   WHERE partition_date = '2000-01-02'
   )
   SELECT * FROM interaccion_repartition ;
   
   
--	 DESCRIBE interaccion_repartition_tmp ;

   
   
SET hive.execution.engine = spark;
SET spark.yarn.queue = root.dataeng;
SET hive.exec.dynamic.partition.mode = nonstrict;
set hive.exec.max.dynamic.partitions=4000;
set hive.exec.max.dynamic.partitions.pernode=4000;

INSERT 	overwrite TABLE bi_corp_common.bt_suc_interaccion PARTITION (partition_date)
SELECT TRIM(TI.cd_interaccion) cod_suc_interaccion
	, CAST(TI.nup AS BIGINT) cod_per_nup
	, TRIM(TI.cd_ejecutivo) cod_suc_legajo
	, to_date(TI.dt_inicio) dt_suc_inicio
	, SUBSTRING(TI.dt_inicio, 1, 19) ts_suc_inicio
	, to_date(TI.dt_cierre) dt_suc_cierre
	, SUBSTRING(TI.dt_cierre, 1, 19) ts_suc_cierre
	, TRIM(TI.cd_canal_comunicacion) cod_suc_canalcomunicacion 
	, TRIM(CC.ds_canal_comunicacion) ds_suc_canalcomunicacion
	, TRIM(TI.cd_canal_venta) cod_suc_canalventa
	, TRIM(CV.ds_canal_venta) ds_suc_canalventa
	, CAST(TI.cd_sucursal AS INT) cod_suc_sucursal
	, TRIM(TI.ds_comentario) ds_suc_comentario
	, TI.p_date partition_date
FROM interaccion_repartition_tmp TI 
LEFT JOIN bi_corp_staging.rio151_tbl_canal_comunicacion CC 
	ON TI.cd_canal_comunicacion = CC.cd_canal_comunicacion 
	AND CC.partition_date = '2021-04-22' 
LEFT JOIN bi_corp_staging.rio151_tbl_canal_venta CV
	ON TI.cd_canal_venta = CV.cd_canal_venta 
	AND CV.partition_date = '2021-04-22' ; 
   

SHOW PARTITIONS bi_corp_common.bt_suc_interaccion ;

SELECT dt_suc_inicio 
	, count(distinct cod_per_nup) clientes
FROM bi_corp_common.bt_suc_interaccion 
WHERE SUBSTRING(dt_suc_inicio, 1, 7) IN ('2020-10','2021-11','2020-12')
GROUP BY dt_suc_inicio ;

-------------------------------------------------------------------------------------

--DESCRIBE bi_corp_staging.rio151_tbl_interaccion_producto  ;

set hive.execution.engine = spark;
set spark.yarn.queue = root.dataeng;
set hive.exec.dynamic.partition.mode = nonstrict;

   DROP TABLE IF EXISTS producto_repartition_tmp ;
   CREATE TEMPORARY TABLE producto_repartition_tmp AS
   WITH producto_repartition AS (
   
	   SELECT cd_interaccion_producto
			, cd_interaccion
			, id_acc
			, ds_campana_o_producto
			, mc_campana
			, cd_escenario
			, cd_envio
			, ds_producto_crm
			, cd_gestion
			, cd_resultado
			, json
			, ds_comentario
			, mc_envio_mail
			, dt_agenda_horario
			, dt_modificacion
			, dt_gestion
			, cd_identificacion_resultado
			, cd_producto_crm
			, vl_nro_solicitud
			, cd_canal_solicitud
			, id_camp_buc
			, ds_contacto
			, seguimientotec 
	   		, to_date(dt_gestion) p_date
	   FROM bi_corp_staging.rio151_tbl_interaccion_producto 
	   WHERE partition_date = '2000-01-01'
	   AND SUBSTRING(dt_gestion, 1, 7) = '2021-11'--IN ('2020-10','2021-11','2020-12')
   )
   SELECT * FROM producto_repartition ;
  
  DESCRIBE 
  
  
set hive.execution.engine = spark;
set spark.yarn.queue = root.dataeng;
set hive.exec.dynamic.partition.mode = nonstrict;
set hive.exec.max.dynamic.partitions=3000;
set hive.exec.max.dynamic.partitions.pernode=3000;

INSERT 	overwrite TABLE bi_corp_common.bt_suc_interaccionproducto PARTITION (partition_date)

SELECT DISTINCT TRIM(IP.cd_interaccion_producto) cod_suc_interaccionproducto
	, TRIM(IP.cd_interaccion) cod_suc_interaccion
	, CAST(IP.id_acc AS INT) cod_suc_acc
	, TRIM(IP.ds_campana_o_producto) ds_suc_campanaoproducto 
	, IF(TRIM(IP.mc_campana) = 'S',1,0) flag_suc_campana
	, CAST(IP.cd_escenario AS INT) cod_suc_escenario
	, CAST(IP.cd_envio AS INT) cod_suc_envio
	, TRIM(IP.ds_producto_crm) ds_suc_productocrm
	, CAST(IP.cd_gestion AS INT) cod_suc_gestion
	, CAST(IP.cd_resultado AS INT) cod_suc_resultado
	, TRIM(IP.json) ds_suc_json
	, TRIM(IP.ds_comentario) ds_suc_comentario
	, IF(TRIM(IP.mc_envio_mail) = 'S',1,0) flag_suc_enviomail
	, to_date(IP.dt_agenda_horario) dt_suc_agendahorario
	, SUBSTRING(IP.dt_agenda_horario, 1, 19) ts_suc_agendahorario
	, to_date(IP.dt_modificacion) dt_suc_modificacion
	, SUBSTRING(IP.dt_modificacion, 1, 19) ts_suc_modificacion
	, to_date(IP.dt_gestion) dt_suc_gestion
	, SUBSTRING(IP.dt_gestion, 1, 19) ts_suc_gestion
	, CAST(IP.cd_identificacion_resultado AS INT) cod_suc_identificacionresultado
	, TRIM(IP.cd_producto_crm) cod_suc_productocrm
	, CAST(IP.vl_nro_solicitud AS INT) cod_suc_vlnumerosolicitud
	, TRIM(IP.cd_canal_solicitud) cod_suc_canalsolicitud
	, TRIM(IP.id_camp_buc) cod_suc_campbuc
	, TRIM(IP.ds_contacto) ds_suc_contacto
	, IF(TRIM(IP.seguimientotec) = 'S',1,0) flag_suc_seguimientotec
	, TRIM(UPPER(GE.grupo)) ds_suc_gestion 
	, TRIM(UPPER(GE.descripcion)) ds_suc_motivo
	, IP.p_date partition_date
FROM producto_repartition_tmp IP
LEFT JOIN bi_corp_staging.rio151_tbl_combo_iatx_tmk GE 
	ON TRIM(IP.cd_gestion) = TRIM(GE.campo1)
	AND TRIM(IP.cd_identificacion_resultado) = TRIM(GE.campo3)
	AND GE.partition_date = '2021-04-26' ; 
  

SELECT dt_suc_modificacion 
	, count(distinct cod_suc_interaccion) clientes
FROM bi_corp_common.bt_suc_interaccionproducto 
WHERE SUBSTRING(dt_suc_modificacion, 1, 7) IN ('2020-10','2020-11','2020-12')
GROUP BY dt_suc_modificacion ;


-----------------------------


DESCRIBE bi_corp_staging.rio154_enc_turno 

set hive.execution.engine = spark;
set spark.yarn.queue = root.dataeng;
set hive.exec.dynamic.partition.mode = nonstrict;

   DROP TABLE IF EXISTS turno_repartition_tmp ;
   CREATE TEMPORARY TABLE turno_repartition_tmp AS
   WITH turno_repartition AS (
   
	   SELECT id_turno
			, id_estado
			, tipodoc
			, nrodoc
			, sector
			, id_suc
			, fecha_hora_desde
			, fecha_hora_hasta
			, fraccion
			, dia
			, nup
			, apellido
			, nombre
			, pesubseg
			, pesexper
			, id_motivo
			, mail
			, cuit
			, id_turno_orig
			, id_ejec_atencion
			, id_suc_atencion
			, id_puesto_citas
			, telefono
			, id_tipo_turno
			, fecha_desde
			, fecha_hasta 
	   		, CONCAT(SUBSTRING(fecha_hora_desde, 1, 4), '-', SUBSTRING(fecha_hora_desde, 5, 2), '-', SUBSTRING(fecha_hora_desde, 7, 2)) p_date
	   FROM bi_corp_staging.rio154_enc_turno 
	   WHERE partition_date = '2000-01-01'
	   AND SUBSTRING(fecha_hora_desde, 1, 6) = '202011'--IN ('2020-10','2021-11','2020-12')
   )
   SELECT * FROM turno_repartition ;
  
  SELECT DISTINCT p_date FROM turno_repartition_tmp ;
  
  
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
	, ET.p_date partition_date
FROM turno_repartition_tmp ET 
-------------------------------------------------------ESTADO
LEFT JOIN bi_corp_staging.rio154_enc_estado EE 
	ON ET.id_estado = EE.id_estado
	AND EE.partition_date = '2021-04-28'
-------------------------------------------------------MOTIVO
LEFT JOIN bi_corp_staging.rio154_motivo_visita_v2 MV 
	ON ET.id_motivo = MV.id_motivo
	AND MV.partition_date = '2021-04-28'
-------------------------------------------------------SUCURSAL
LEFT JOIN bi_corp_staging.rio154_sucursalrio SUC 
	ON ET.id_suc = SUC.codigo 
	AND SUC.partition_date = '2021-04-28'
-------------------------------------------------------DS_TIPODOC
LEFT JOIN 
	(SELECT DISTINCT TRIM(substr(gen_clave, 1, 1)) tipodoc, gen_datos ds_tipodoc
	 FROM bi_corp_staging.tcdtgen WHERE 
	 partition_date = '2021-04-28' AND gentabla = '0113') DS
	ON TRIM(ET.tipodoc) = DS.tipodoc ;
  
  
  
SELECT partition_date 
	, count(distinct cod_suc_turno) turnos
FROM bi_corp_common.bt_suc_turnero
WHERE SUBSTRING(partition_date, 1, 7) = '2020-11'
GROUP BY partition_date ;

SELECT DISTINCT SUBSTRING(fecha_hora_desde, 1, 6)
FROM bi_corp_staging.rio154_enc_turno 
	   WHERE partition_date = '2000-01-01' ORDER BY 1 DESC
	   
	   


DESCRIBE bi_corp_common.stk_cys_relacioncuentasciti ;


SELECT * FROM bi_corp_common.stk_cys_relacioncuentasciti ;

-- DROP TABLE bi_corp_common.stk_cys_relacioncuentasciti ;
CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_common.stk_cys_relacioncuentasciti (
  cod_cys_entidad STRING ,
  cod_suc_sucursal INT ,
  cod_nro_cuenta BIGINT ,
  cod_prod_producto STRING ,
  cod_prod_subproducto STRING ,
  cod_div_divisa STRING ,
  cod_suc_sucursalorigen INT ,
  cod_nro_cuentaorigen BIGINT ,
  cod_cys_entidadorigen STRING )
PARTITIONED BY (partition_date string)
STORED AS PARQUET
LOCATION '/santander/bi-corp/common/cys/stk_cys_relacioncuentasciti' ;

set hive.exec.dynamic.partition.mode=nonstrict;
set mapred.job.queue.name=root.dataeng;
INSERT OVERWRITE TABLE bi_corp_common.stk_cys_relacioncuentasciti
PARTITION(partition_date)

SELECT trim(entidad) cod_cys_entidad
    , CAST(centro AS INT) cod_cys_centro
    , CAST(contrato AS INT) cod_cys_contrato
    , trim(producto) cod_cys_producto
    , trim(subproducto) cod_cys_subproducto
    , trim(divisa) cod_cys_divisa
    , CAST(centro_otro_bco AS INT) cod_cys_centroorigen
    , CAST(contrato_otro_bco AS INT) cod_cys_contratoorigen
    , trim(entidad_origen) cod_cys_entidadorigen
    , partition_date
FROM bi_corp_staging.rio125_rel_contrato_otro_bco ;



SELECT * FROM bi_corp_risk.ajustes_cartera



SELECT * FROM bi_corp_risk.cartera_ajustes 
WHERE partition_date = '2021-04-12' ORDER BY periodo DESC ;



