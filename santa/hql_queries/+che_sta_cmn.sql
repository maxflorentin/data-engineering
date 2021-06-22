
--               _          
--      _    ___| |__   ___ 
--    _| |_ / __| '_ \ / _ \
--   |_   _| (__| | | |  __/
--     |_|  \___|_| |_|\___|
--                          

SELECT COUT(1) FROM bi_corp_common.bt_cc_mascheinteraccion WHERE partition_date = '2020-11-18' ;

--__/__/__/__/__/__/__/__/__/__/__/__/__/__/__/__/__/__/__/__/__/__/__/__/

CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_common.bt_suc_interaccion ( 
	cod_suc_interaccion string,
	cod_per_nup bigint,
	cod_suc_legajo string,
	dt_suc_inicio string,
	ts_suc_inicio timestamp,
	dt_suc_cierre string,
	ts_suc_cierre timestamp,
	cod_suc_canalcomunicacion string,
	ds_suc_canalcomunicacion string,
	cod_suc_canalventa string,
	ds_suc_canalventa string,
	cod_suc_sucursal int,
	ds_suc_comentario string ) 
PARTITIONED BY ( partition_date string) 
STORED AS PARQUET 
LOCATION '/santander/bi-corp/common/interacciones_canales/sucursal/fact/bt_suc_interaccion'


SET hive.execution.engine = spark;
SET spark.yarn.queue = root.dataeng;
SET hive.exec.dynamic.partition.mode = nonstrict;

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
	, TI.partition_date
FROM bi_corp_staging.rio151_tbl_interaccion TI 
LEFT JOIN bi_corp_staging.rio151_tbl_canal_comunicacion CC 
	ON TI.cd_canal_comunicacion = CC.cd_canal_comunicacion 
	AND TI.partition_date = CC.partition_date 
LEFT JOIN bi_corp_staging.rio151_tbl_canal_venta CV
	ON TI.cd_canal_venta = CV.cd_canal_venta 
	AND TI.partition_date = CV.partition_date 
WHERE TI.partition_date = '2020-11-23' ;

--__/__/__/__/__/__/__/__/__/__/__/__/__/__/__/__/__/__/__/__/__/__/__/__/

SELECT * FROM bi_corp_common.bt_suc_interaccion WHERE partition_date = '2020-11-23' LIMIT 10 ;

--__/__/__/__/__/__/__/__/__/__/__/__/__/__/__/__/__/__/__/__/__/__/__/__/


SHOW PARTITIONS bi_corp_staging.rio151_tbl_interaccion ;

DESCRIBE bi_corp_staging.rio151_tbl_interaccion_alerta ;

--__/__/__/__/__/__/__/__/__/__/__/__/__/__/__/__/__/__/__/__/__/__/__/__/

CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_common.bt_suc_interaccionalerta ( 
	cod_suc_interaccionalerta string,
	cod_suc_interaccion string,
	cod_suc_alerta int,
	cod_suc_resultado int,
	cod_suc_gestion int,
	dt_suc_gestion string,
	ts_suc_gestion timestamp,
	ds_suc_claveunica string,
	ds_suc_json string,
	ds_suc_contacto string,
	cod_suc_identificacionresultado int) 
PARTITIONED BY ( partition_date string) 
STORED AS PARQUET 
LOCATION '/santander/bi-corp/common/interacciones_canales/sucursal/fact/bt_suc_interaccionalerta'


SET hive.execution.engine = spark;
SET spark.yarn.queue = root.dataeng;
SET hive.exec.dynamic.partition.mode = nonstrict;

INSERT 	overwrite TABLE bi_corp_common.bt_suc_interaccionalerta PARTITION (partition_date)
SELECT TRIM(cd_interaccion_alerta) cod_suc_interaccionalerta
	, TRIM(cd_interaccion) cod_suc_interaccion
	, CAST(cd_alerta AS INT) cod_suc_alerta
	, CAST(cd_resultado AS INT) cod_suc_resultado
	, CAST(cd_gestion AS INT) cod_suc_gestion
	, to_date(dt_gestion) dt_suc_gestion
	, SUBSTRING(dt_gestion, 1, 19) ts_suc_gestion
	, TRIM(clave_unica) ds_suc_claveunica
	, TRIM(json) ds_suc_json
	, TRIM(ds_contacto) ds_suc_contacto
	, CAST(cd_identificacion_resultado AS INT) cod_suc_identificacionresultado
	, partition_date
FROM bi_corp_staging.rio151_tbl_interaccion_alerta 
WHERE partition_date = '2020-11-23' ;


--__/__/__/__/__/__/__/__/__/__/__/__/__/__/__/__/__/__/__/__/__/__/__/__/

SELECT * FROM bi_corp_common.bt_suc_interaccionalerta WHERE partition_date = '2020-11-23' LIMIT 10 ;

--__/__/__/__/__/__/__/__/__/__/__/__/__/__/__/__/__/__/__/__/__/__/__/__/

-- DROP TABLE bi_corp_common.bt_suc_interaccionproducto ;
CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_common.bt_suc_interaccionproducto ( 
	cod_suc_interaccionproducto string,
	cod_suc_interaccion string,
	cod_suc_acc int,
	ds_suc_campanaoproducto string,
	flag_suc_campana int,
	cod_suc_escenario int,
	cod_suc_envio int,
	ds_suc_productocrm string,
	cod_suc_gestion int,
	cod_suc_resultado int,
	ds_suc_json string,
	ds_suc_comentario string,
	flag_suc_enviomail int,
	dt_suc_agendahorario string,
	ts_suc_agendahorario timestamp,
	dt_suc_modificacion string,
	ts_suc_modificacion timestamp,
	dt_suc_gestion string,
	ts_suc_gestion timestamp,
	cod_suc_identificacionresultado int,
	cod_suc_productocrm string,
	cod_suc_vlnumerosolicitud int,
	cod_suc_canalsolicitud string,
	cod_suc_campbuc string, 
	ds_suc_contacto string,
	flag_suc_seguimientotec int	) 
PARTITIONED BY ( partition_date string) 
STORED AS PARQUET 
LOCATION '/santander/bi-corp/common/interacciones_canales/sucursal/fact/bt_suc_interaccionproducto'


SET hive.execution.engine = spark;
SET spark.yarn.queue = root.dataeng;
SET hive.exec.dynamic.partition.mode = nonstrict;

INSERT 	overwrite TABLE bi_corp_common.bt_suc_interaccionproducto PARTITION (partition_date)
SELECT TRIM(cd_interaccion_producto) cod_suc_interaccionproducto
	, TRIM(cd_interaccion) cod_suc_interaccion
	, CAST(id_acc AS INT) cod_suc_acc
	, TRIM(ds_campana_o_producto) ds_suc_campanaoproducto 
	, IF(TRIM(mc_campana) = 'S',1,0) flag_suc_campana
	, CAST(cd_escenario AS INT) cod_suc_escenario
	, CAST(cd_envio AS INT) cod_suc_envio
	, TRIM(ds_producto_crm) ds_suc_productocrm
	, CAST(cd_gestion AS INT) cod_suc_gestion
	, CAST(cd_resultado AS INT) cod_suc_resultado
	, TRIM(json) ds_suc_json
	, TRIM(ds_comentario) ds_suc_comentario
	, IF(TRIM(mc_envio_mail) = 'S',1,0) flag_suc_enviomail
	, to_date(dt_agenda_horario) dt_suc_agendahorario
	, SUBSTRING(dt_agenda_horario, 1, 19) ts_suc_agendahorario
	, to_date(dt_modificacion) dt_suc_modificacion
	, SUBSTRING(dt_modificacion, 1, 19) ts_suc_modificacion
	, to_date(dt_gestion) dt_suc_gestion
	, SUBSTRING(dt_gestion, 1, 19) ts_suc_gestion
	, CAST(cd_identificacion_resultado AS INT) cod_suc_identificacionresultado
	, TRIM(cd_producto_crm) cod_suc_productocrm
	, CAST(vl_nro_solicitud AS INT) cod_suc_vlnumerosolicitud
	, TRIM(cd_canal_solicitud) cod_suc_canalsolicitud
	, TRIM(id_camp_buc) cod_suc_campbuc
	, TRIM(ds_contacto) ds_suc_contacto
	, IF(TRIM(seguimientotec) = 'S',1,0) flag_suc_seguimientotec
	, partition_date
FROM bi_corp_staging.rio151_tbl_interaccion_producto 
WHERE partition_date = '2020-11-23' ;


--__/__/__/__/__/__/__/__/__/__/__/__/__/__/__/__/__/__/__/__/__/__/__/__/

SELECT * FROM bi_corp_common.bt_suc_interaccionproducto WHERE partition_date = '2020-11-23'AND cod_suc_interaccionproducto = '22781315' ;

--__/__/__/__/__/__/__/__/__/__/__/__/__/__/__/__/__/__/__/__/__/__/__/__/







------------------------------------------------------------------------------------------
-- TABLA ALHA

SELECT DISTINCT supra_source FROM santander_business_risk_arda.saldos_altair_mes_actual ;

------------------------------------------------------------------------------------------


SELECT * FROM bi_corp_common.bt_cc_maschetrackeo WHERE partition_date = '2020-11-26' LIMIT 20 ;

SELECT * FROM bi_corp_common.bt_cc_maschetransaccion WHERE partition_date = '2020-11-26' LIMIT 20 ;

DESCRIBE bi_corp_staging.rio151_tbl_interaccion_producto 

SELECT * FROM bi_corp_staging.rio56_sec

SHOW PARTITIONS bi_corp_common.stk_bcaaut_remanentes ;