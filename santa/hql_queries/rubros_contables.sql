

-- renumeraciones:

set hive.execution.engine=spark;
set mapred.job.queue.name=root.dataeng;
set hive.exec.dynamic.partition.mode=nonstrict;

DROP TABLE renumeraciones ;
CREATE TEMPORARY TABLE renumeraciones AS
WITH 

	cuentas_previousdate AS (

		SELECT DISTINCT mig_new_cent_alta
			, mig_new_cuenta
			, mig_new_divisa
		FROM bi_corp_staging.malbgc_'ZB'dtmig
		WHERE partition_date = '2021-05-11'	
		AND 

	)
	
	, cuentas_thisdate AS (
	
		SELECT T.mig_new_cent_alta ,
			T.mig_new_cuenta ,
			T.mig_new_divisa ,
			T.mig_old_cent_alta ,
			T.mig_old_cuenta ,
			T.mig_old_divisa ,
			T.mig_old_indesta ,
			T.mig_old_motiv_baja ,
			T.mig_old_motiv_migr ,
			T.partition_date
		FROM bi_corp_staging.malbgc_'ZB'dtmig T
		LEFT JOIN cuentas_previousdate P
			ON T.mig_new_cent_alta = P.mig_new_cent_alta
			AND T.mig_new_cuenta = P.mig_new_cuenta
			AND T.mig_new_divisa = P.mig_new_divisa 
		WHERE partition_date = '2020-08-10'	
			AND P.mig_new_cuenta IS NULL
	)

	, new_con AS (
	
		SELECT T.mig_new_cent_alta ,
			T.mig_new_cuenta ,
			T.mig_new_divisa ,
			T.mig_old_cent_alta ,
			T.mig_old_cuenta ,
			T.mig_old_divisa ,
			T.mig_old_indesta ,
			T.mig_old_motiv_baja ,
			T.mig_old_motiv_migr ,
			P8.new_penumper ,
			P8.new_pecodpro ,
			P8.new_pecodsub ,
			P8.new_penumcon ,
			P8.new_sucursal ,
			P8.new_pefecalt ,
			P8.new_pefecbrb ,
			T.partition_date
		FROM cuentas_thisdate T  	
		LEFT JOIN (
					SELECT
						penumper new_penumper ,
						pecodpro new_pecodpro ,
						pecodsub new_pecodsub ,
						partition_date ,
						CAST(penumcon AS bigint) new_penumcon ,
						pecodofi new_sucursal , 
						pefecalt new_pefecalt ,
						pefecbrb new_pefecbrb 
					FROM
						bi_corp_staging.malpe_pedt008
					WHERE partition_date = '2020-08-10'
						AND pecalpar = 'TI' 
				) P8 
				ON  CAST(P8.new_penumcon AS BIGINT) = CAST(substring(T.mig_new_cuenta, 4, 9) AS BIGINT)
				AND P8.new_sucursal = T.mig_new_cent_alta
			WHERE (SUBSTRING(T.mig_new_cuenta, 2, 2) = P8.new_pecodpro 
					OR P8.new_pecodpro = '70')
		
	)	
	
	SELECT T.mig_new_cent_alta ,
			T.mig_new_cuenta ,
			T.mig_new_divisa ,
			T.mig_old_cent_alta ,
			T.mig_old_cuenta ,
			T.mig_old_divisa ,
			T.mig_old_indesta ,
			T.mig_old_motiv_baja ,
			T.mig_old_motiv_migr ,
			T.new_penumper ,
			T.new_pecodpro ,
			T.new_pecodsub ,
			T.new_penumcon ,
			T.new_sucursal ,
			T.new_pefecalt ,
			T.new_pefecbrb ,	
			P8b.old_penumper ,
			P8b.old_pecodpro ,
			P8b.old_pecodsub ,
			P8b.old_penumcon ,
			P8b.old_sucursal ,
			P8b.old_pefecalt ,
			P8b.old_pefecbrb ,
			T.partition_date
	FROM  new_con T			
	LEFT JOIN (
				SELECT
					penumper old_penumper ,
					pecodpro old_pecodpro ,
					pecodsub old_pecodsub ,
					partition_date ,
					CAST(penumcon AS bigint) old_penumcon , 
					pecodofi old_sucursal ,
					pefecalt old_pefecalt ,
					pefecbrb old_pefecbrb
				FROM
					bi_corp_staging.malpe_pedt008
				WHERE partition_date = '2020-08-10'
					AND pecalpar = 'TI' 
				) P8b 
			ON  CAST(P8b.old_penumcon AS BIGINT) = CAST(substring(T.mig_old_cuenta, 4, 9) AS BIGINT)
			AND P8b.old_sucursal = T.mig_old_cent_alta
		WHERE (SUBSTRING(T.mig_old_cuenta, 2, 2) = P8b.old_pecodpro 
			OR P8b.old_pecodpro != '70')  ;


SELECT DISTINCT * FROM renumeraciones WHERE new_penumper = '06242016' ; --'01316873' ; '06242016'

	
-----------------------

-- SHOW PARTITIONS bi_corp_staging.malbgc_'ZB'dtmig ;

set hive.execution.engine=spark;
set mapred.job.queue.name=root.dataeng;
set hive.exec.dynamic.partition.mode=nonstrict;

	SELECT partition_date , count(1) registros
	FROM bi_corp_staging.malbgc_'ZB'dtmig 
	GROUP BY partition_date ;


SELECT DISTINCT mig_new_entidad
	, mig_new_cent_alta
	, mig_new_cuenta
	, mig_new_divisa
	, mig_old_fech_baja
FROM bi_corp_staging.malbgc_'ZB'dtmig 
WHERE partition_date = '2019-12-16'	;

SELECT partition_date , mig_old_fech_baja -- 1980-06-16 hasta 
	, COUNT(DISTINCT mig_new_cuenta) cuentas
FROM bi_corp_staging.malbgc_'ZB'dtmig
WHERE partition_date = '2019-12-16'
AND mig_old_fech_baja  = '0001-01-01'
GROUP BY partition_date ,mig_old_fech_baja

UNION all

SELECT partition_date ,mig_old_fech_baja -- 1980-06-16 hasta 
	, COUNT(DISTINCT mig_new_cuenta) cuentas
FROM bi_corp_staging.malbgc_'ZB'dtmig
WHERE partition_date = '2019-12-23'
AND mig_old_fech_baja = '0001-01-01'
GROUP BY partition_date ,mig_old_fech_baja

UNION all

SELECT partition_date  -- 1980-06-16 hasta 
	, COUNT(DISTINCT mig_new_cuenta) cuentas
FROM bi_corp_staging.malbgc_'ZB'dtmig
WHERE mig_old_fech_baja = '0001-01-01'
GROUP BY partition_date ;






--007000247993
--007000247993
--007003682157
--007003509948
set hive.execution.engine=spark;
set mapred.job.queue.name=root.dataeng;
set hive.exec.dynamic.partition.mode=nonstrict;
DROP TABLE IF EXISTS renumeracion ;
CREATE TEMPORARY TABLE renumeracion AS
SELECT CAST(mig.mig_new_cent_alta AS INT) cod_suc_sucursalmig
	, CAST(mig.mig_new_cuenta AS BIGINT) cod_nro_cuentamig
	, TRIM(mig.mig_new_divisa) cod_div_divisamig
	, CAST(mig.mig_old_cent_alta AS INT) cod_suc_sucursalold
	, CAST(mig.mig_old_cuenta AS BIGINT) cod_nro_cuentaold
	, TRIM(mig.mig_old_divisa) cod_div_divisaold
	, TRIM(mig.mig_old_indesta) cod_cys_indestaold
	, TRIM(mig.mig_old_motiv_baja) cod_cys_motivobajaold
	, TRIM(mig.mig_old_motiv_migr) cod_cys_motivomigrold
	--, pdt.penumcon
	, CAST(newp.penumper AS BIGINT) cod_per_nup
	, newp.pecodpro cod_prod_productomig
	, newp.pecodsub cod_prod_su'BP'roductomig
	--, pdt.peestrel
	--, pdt.pemarpaq
	, oldp.pecodpro cod_prod_productoold
	, oldp.pecodsub cod_prod_su'BP'roductoold
	, mig.
FROM bi_corp_staging.malbgc_'ZB'dtmig mig
-- new
LEFT JOIN bi_corp_staging.malpe_pedt008 newp 
	ON newp.pecalpar = 'TI'
	AND newp.partition_date = '2019-12-17'
	AND newp.pefecalt = mig.mig_old_fech_baja
	AND CAST(newp.penumcon AS BIGINT) = CAST(substring(mig.mig_new_cuenta, 4, 9) AS BIGINT)
	AND CAST(newp.pecodofi AS INT) = CAST(mig.mig_new_cent_alta AS INT)
 	AND newp.pemarpaq = 'S'
-- old
LEFT JOIN bi_corp_staging.malpe_pedt008 oldp 
	ON oldp.pecalpar = 'TI'
	AND oldp.partition_date = '2019-12-17'
	AND oldp.pefecalt = mig.mig_old_fech_baja 
	AND CAST(oldp.penumcon AS BIGINT) = CAST(substring(mig.mig_old_cuenta, 4, 9) AS BIGINT)
	AND CAST(oldp.pecodofi AS INT) = CAST(mig.mig_old_cent_alta AS INT)
 	--AND oldp.pemarpaq = 'S'
WHERE mig.partition_date = '2019-12-17'
AND newp.penumper = '06242016' 
--AND CAST(substring(mig.mig_new_cuenta, 4, 9) AS BIGINT) = 3890330
--AND CAST(pdt.penumcon AS BIGINT) IS NOT NULL ;

SELECT * FROM renumeracion ; 

-- 007003509948 - 2019-12-13
-- 007003509948 - 2019-12-13
-- 007000247993 - 2019-12-17 --
-- 007000247993 - 2019-12-17 --
-- 007003682157 - 2017-01-06
-- 007003682157 - 2017-01-06

-- 3 : 2019-12-13 - 2019-12-17 - 2017-01-06
-- COUNT(1): 4.578.904
SELECT *
FROM bi_corp_staging.malbgc_'ZB'dtmig mig
WHERE mig.partition_date = '2021-05-12'
AND CAST(substring(mig.mig_new_cuenta, 4, 9) AS BIGINT) = 3890330 ;

-- 6 registros
SELECT *
FROM bi_corp_staging.malpe_pedt008
WHERE substring(partition_date, 1, 4) = '2017'
--AND penumper = '06242016' 
AND penumcon = '007003509948' -- '000003890330' -- '007003509948' '007003682157'--
--AND pecalpar = 'TI' ;


SELECT * FROM bi_corp_staging.adsf_cartera
WHERE partition_date = '2021-05-03' ;








SHOW PARTITIONS bi_corp_staging.tcdtgen ;
-- partition_date=2021-05-18

SHOW PARTITIONS bi_corp_staging.afir_cod_causales ;


SHOW PARTITIONS bi_corp_staging.afir_in00 ;
-- partition_date=2021-03-31
-- partition_date=2021-04-30

SHOW PARTITIONS bi_corp_common.stk_cys_inhabilitadosrisk ;


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
	 FROM bi_corp_staging.tcdtgen WHERE partition_date = ''
	 AND gentabla = '0113') DS
	ON TRIM(INH.tpo_doc) = DS.tipodoc 
LEFT JOIN bi_corp_staging.afir_cod_causales CA 
	ON TRIM(INH.cod_caus) = TRIM(CA.cod_caus)
WHERE INH.partition_date = '2021-04-30' 
AND TRIM(INH.cod_caus) IN ('017','018','019') ;

INSERT INTO TABLE bi_corp_staging.rio350_rel_sucursal_zona
SELECT 809,'10'

SELECT * FROM bi_corp_staging.rio350_rel_sucursal_zona WHERE cod_suc_sucursal = 809

SELECT * 
FROM bi_corp_staging.rio350_sucursales
WHERE partition_date = '2021-05-18'



CREATE TEMPORARY TABLE sucursales AS
WITH servicios AS (

		SELECT sus.sucu_id
			, concat_ws(', ',collect_list(DISTINCT ser.servicio_nombre)) as servicios
			, concat_ws(', ',collect_list(DISTINCT CONCAT(osu.hora_desde, '-', osu.hora_hasta))) as horario
		FROM bi_corp_staging.rio350_sucursal_x_serv sus 
		LEFT JOIN bi_corp_staging.rio350_servicios ser 
			ON ser.partition_date = '2021-05-18'
			AND sus.servicio_id = ser.servicio_id
		LEFT JOIN bi_corp_staging.rio350_sucursal_x_opening_hour_sucu hsu
			ON hsu.partition_date = '2021-05-18'
			AND sus.sucu_id = sus.sucu_id
		LEFT JOIN bi_corp_staging.rio350_opening_hour_sucu osu
			ON osu.partition_date = '2021-05-18'
			AND hsu.opening_hour_sucu_id = osu.opening_hour_sucu_id
			AND hsu.sucu_id = sus.sucu_id
		GROUP BY sus.sucu_id

)

SELECT CAST(suc.ref_legacy AS INT) cod_suc_sucursal --nro de sucursal
	, suc.sucu_nombre ds_suc_sucursal--nombre sucursal
	, zon.cod_suc_zona --zona
	, NULL ds_suc_zona --nombre zona
	, dir.calle ds_suc_domiciliocalle --domicilio sucursal calle
	, dir.numero ds_suc_domicilionumero --domicilio sucursal altura
	, dir.barrio ds_suc_localidad --domicilio sucursal localidad
	, pro.nombre ds_suc_provincia-- provincia
	, dir.cod_postal cod_suc_codpostal --domicilio sucursal codigo postal
	, coo.latitud ds_suc_latitud --domicilio sucursal latitud
	, coo.longitud ds_suc_longitud --domicilio sucursal longitud
	, CONCAT(tel.cod_area, '-', tel.numero) ds_suc_telefono--telefono
	, srv.horario ds_suc_horario --horario de atencion al publico
	, NULL ds_suc_gerente-- nombre y apellido del gerente
	, IF(suc.habilitado = '1', 'ABIERTA', 'CERRADA') ds_suc_estado  --Estado (abierta / Cerrada)
	, NULL dt_suc_fechaalta --Fecha alta
	, NULL dt_suc_fechamodif--fecha modificacion
	, NULL dt_suc_fechabaja--fecha baja
	, srv.servicios ds_suc_servicios
	--tipo de sucursal (simplificada, full, etc)
FROM bi_corp_staging.rio350_sucursales suc 
LEFT JOIN bi_corp_staging.rio350_rel_sucursal_zona zon 
	ON zon.cod_suc_sucursal = CAST(suc.ref_legacy AS INT)
LEFT JOIN bi_corp_staging.rio350_telefonos tel 
	ON tel.sucu_id = suc.sucu_id
	AND tel.partition_date = '2021-05-18'
	AND tel.numero != '0000000'
LEFT JOIN servicios srv 
	ON srv.sucu_id = suc.sucu_id
LEFT JOIN bi_corp_staging.rio350_direcciones dir 
	ON dir.sucu_id = suc.sucu_id
	AND dir.partition_date = '2021-05-18'
LEFT JOIN bi_corp_staging.rio350_coordenadas coo 
	ON coo.coordenada_id = suc.sucu_id
	AND coo.partition_date = '2021-05-18'
LEFT JOIN bi_corp_staging.rio350_provincias pro 
	ON pro.provincia_id = dir.provincia_id
	AND pro.partition_date = '2021-05-18'
WHERE suc.partition_date = '2021-05-18' ;


-------


CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_common.stk_suc_sucursales (
	cod_suc_sucursal	int,
	ds_suc_sucursal	string,
	cod_suc_zona	string,
	ds_suc_zona	string,
	ds_suc_domiciliocalle	string,
	ds_suc_domicilionumero	string,
	ds_suc_localidad	string,
	ds_suc_provincia	string,
	cod_suc_codpostal	string,
	ds_suc_latitud	string,
	ds_suc_longitud	string,
	ds_suc_telefono	string,
	ds_suc_horario	string,
	ds_suc_gerente	string,
	ds_suc_estado	string,
	dt_suc_fechaalta	string,
	dt_suc_fechamodif	string,
	dt_suc_fechabaja	string,
	ds_suc_servicios	string
)
PARTITIONED BY ( partition_date string) 
STORED AS PARQUET 
LOCATION '/santander/bi-corp/common/interacciones_canales/sucursal/fact/stk_suc_sucursales'


SET hive.execution.engine = spark;
SET spark.yarn.queue = root.dataeng;
SET hive.exec.dynamic.partition.mode = nonstrict;

WITH servicios AS (

		SELECT sus.sucu_id
			, concat_ws(', ',collect_list(DISTINCT ser.servicio_nombre)) as servicios
			, concat_ws(', ',collect_list(DISTINCT CONCAT(osu.hora_desde, '-', osu.hora_hasta))) as horario
		FROM bi_corp_staging.rio350_sucursal_x_serv sus 
		LEFT JOIN bi_corp_staging.rio350_servicios ser 
			ON ser.partition_date = '2021-05-18'
			AND sus.servicio_id = ser.servicio_id
		LEFT JOIN bi_corp_staging.rio350_sucursal_x_opening_hour_sucu hsu
			ON hsu.partition_date = '2021-05-18'
			AND sus.sucu_id = sus.sucu_id
		LEFT JOIN bi_corp_staging.rio350_opening_hour_sucu osu
			ON osu.partition_date = '2021-05-18'
			AND hsu.opening_hour_sucu_id = osu.opening_hour_sucu_id
			AND hsu.sucu_id = sus.sucu_id
		GROUP BY sus.sucu_id

)

INSERT 	overwrite TABLE bi_corp_common.stk_suc_sucursales PARTITION (partition_date)
SELECT CAST(suc.ref_legacy AS INT) cod_suc_sucursal --nro de sucursal
	, suc.sucu_nombre ds_suc_sucursal--nombre sucursal
	, zon.cod_suc_zona --zona
	, NULL ds_suc_zona --nombre zona
	, dir.calle ds_suc_domiciliocalle --domicilio sucursal calle
	, dir.numero ds_suc_domicilionumero --domicilio sucursal altura
	, dir.barrio ds_suc_localidad --domicilio sucursal localidad
	, pro.nombre ds_suc_provincia-- provincia
	, dir.cod_postal cod_suc_codpostal --domicilio sucursal codigo postal
	, coo.latitud ds_suc_latitud --domicilio sucursal latitud
	, coo.longitud ds_suc_longitud --domicilio sucursal longitud
	, CONCAT(tel.cod_area, '-', tel.numero) ds_suc_telefono--telefono
	, srv.horario ds_suc_horario --horario de atencion al publico
	, NULL ds_suc_gerente-- nombre y apellido del gerente
	, IF(suc.habilitado = '1', 'ABIERTA', 'CERRADA') ds_suc_estado  --Estado (abierta / Cerrada)
	, NULL dt_suc_fechaalta --Fecha alta
	, NULL dt_suc_fechamodif--fecha modificacion
	, NULL dt_suc_fechabaja--fecha baja
	, srv.servicios ds_suc_servicios
	, suc.partition_date
FROM bi_corp_staging.rio350_sucursales suc 
LEFT JOIN bi_corp_staging.rio350_rel_sucursal_zona zon 
	ON zon.cod_suc_sucursal = CAST(suc.ref_legacy AS INT)
LEFT JOIN bi_corp_staging.rio350_telefonos tel 
	ON tel.sucu_id = suc.sucu_id
	AND tel.partition_date = '2021-05-18'
	AND tel.numero != '0000000'
LEFT JOIN servicios srv 
	ON srv.sucu_id = suc.sucu_id
LEFT JOIN bi_corp_staging.rio350_direcciones dir 
	ON dir.sucu_id = suc.sucu_id
	AND dir.partition_date = '2021-05-18'
LEFT JOIN bi_corp_staging.rio350_coordenadas coo 
	ON coo.coordenada_id = suc.sucu_id
	AND coo.partition_date = '2021-05-18'
LEFT JOIN bi_corp_staging.rio350_provincias pro 
	ON pro.provincia_id = dir.provincia_id
	AND pro.partition_date = '2021-05-18'
WHERE suc.partition_date = '2021-05-18' ; 

----------------------------

bi_corp_staging.rio350_sucursales
bi_corp_staging.rio350_direcciones
bi_corp_staging.rio350_coordenadas
bi_corp_staging.rio350_provincias
bi_corp_staging.rio350_sucursal_x_serv
SELECT * FROM bi_corp_staging.rio350_servicios WHERE partition_date = '2021-05-18'
bi_corp_staging.rio350_sucursal_x_opening_hour_sucu
bi_corp_staging.rio350_opening_hour_sucu



SHOW PARTITIONS bi_corp_common.stk_suc_sucursales

SELECT * FROM bi_corp_common.stk_suc_sucursales 
WHERE partition_date = '2021-05-18'  AND cod_suc_sucursal = 809 ;


SELECT *
FROM bi_corp_staging.rio350_telefonos 
WHERE partition_date = '2021-05-18'
GROUP BY sucu_id ;

SELECT *
FROM bi_corp_staging.rio350_servicios
WHERE partition_date = '2021-05-18'


SELECT sucu_id , count(servicio_id) 
FROM bi_corp_staging.rio350_sucursal_x_serv
WHERE partition_date = '2021-05-18'
group by sucu_id

SELECT sus.sucu_id
	,concat_ws(', ',collect_list(ser.servicio_nombre)) as servicios
FROM bi_corp_staging.rio350_sucursal_x_serv sus 
LEFT JOIN bi_corp_staging.rio350_servicios ser 
	ON ser.partition_date = '2021-05-18'
	AND sus.servicio_id = ser.servicio_id
GROUP BY sus.sucu_id ;

SELECT *
FROM bi_corp_staging.rio350_direcciones
WHERE partition_date = '2021-05-18'

SELECT *
FROM bi_corp_staging.rio350_sucursales
WHERE partition_date = '2021-05-18'

SELECT *
FROM bi_corp_staging.rio350_sucursal_x_opening_hour_sucu
WHERE partition_date = '2021-05-18'

SELECT *
FROM bi_corp_staging.rio350_opening_hour_sucu
WHERE partition_date = '2021-05-18'

	SELECT sus.sucu_id
		, concat_ws(', ',collect_list(DISTINCT ser.servicio_nombre)) as servicios
		, concat_ws(', ',collect_list(DISTINCT CONCAT(osu.hora_desde, '-', osu.hora_hasta))) as horario
	FROM bi_corp_staging.rio350_sucursal_x_serv sus 
	LEFT JOIN bi_corp_staging.rio350_servicios ser 
		ON ser.partition_date = '2021-05-18'
		AND sus.servicio_id = ser.servicio_id
	LEFT JOIN bi_corp_staging.rio350_sucursal_x_opening_hour_sucu hsu
		ON hsu.partition_date = '2021-05-18'
		AND sus.sucu_id = sus.sucu_id
	LEFT JOIN bi_corp_staging.rio350_opening_hour_sucu osu
		ON osu.partition_date = '2021-05-18'
		AND hsu.opening_hour_sucu_id = osu.opening_hour_sucu_id
		AND hsu.sucu_id = sus.sucu_id
	GROUP BY sus.sucu_id


show partitions bi_corp_common.bt_cys_rubroscontables



SELECT * FROM bi_corp_common.bt_cys_rubroscontables
WHERE partition_date = '2021-05-17'
AND ds_cys_nombrecuenta IS NULL


SELECT *
FROM bi_corp_common.bt_suc_turnero WHERE partition_date >= '2021-05-01'


DESCRIBE 



-----------------








set hive.execution.engine = spark ;
set spark.yarn.queue = root.dataeng ;
set hive.exec.dynamic.partition.mode = nonstrict ;

WITH rubros_contables AS (

	SELECT DISTINCT cod_cys_rubroaltair, partition_date FROM (

	SELECT DISTINCT TRIM(a9601.rubro_altair) cod_cys_rubroaltair , partition_date
	FROM bi_corp_staging.malha_alha9601 a9601 
	WHERE a9601.partition_date = '2021-05-20' 
	UNION ALL 
	SELECT DISTINCT TRIM(a9600.rubro_altair) cod_cys_rubroaltair , partition_date
	FROM bi_corp_staging.malha_alha9600 a9600 
	WHERE a9600.partition_date = '2021-05-20' 
	UNION ALL 
	SELECT DISTINCT TRIM(cuenta_contable_pl1) cod_cys_rubroaltair , partition_date
	FROM bi_corp_staging.malha_hals7770 
	WHERE partition_date = '2021-05-20' 
	)A

	)

	, hals7770 AS (
	
	SELECT rc.cod_cys_rubroaltair 
		, SUM(TRIM(REGEXP_REPLACE(h7770.saldo_mes, ",", "."))) fc_cys_saldomes
		, SUM(TRIM(REGEXP_REPLACE(h7770.saldo_promedio_mes, ",", "."))) fc_cys_saldopromediomes
		, rc.partition_date
	FROM rubros_contables rc
	LEFT JOIN bi_corp_staging.malha_hals7770 h7770
	ON rc.cod_cys_rubroaltair = TRIM(h7770.cuenta_contable_pl1)
	AND h7770.partition_date = '2021-05-20'
	GROUP BY rc.cod_cys_rubroaltair, rc.partition_date 

	)
	

INSERT OVERWRITE TABLE bi_corp_common.bt_cys_rubroscontables
PARTITION(partition_date) 

SELECT TRIM(a9600.entidad) cod_cys_entidad
		, from_unixtime(unix_timestamp(a9600.fecha_informacion ,'dd-mm-yyyy'),'yyyy-mm-dd') dt_cys_fechainformacion
		, COALESCE(h7770.cod_cys_rubroaltair, TRIM(a9600.rubro_altair)) cod_cys_rubroaltair
		, TRIM(a9600.nombre_cuenta) ds_cys_nombrecuenta
		, TRIM(a9600.rubro_bcra) cod_cys_rubrobcra
		, TRIM(a9600.cargabal) cod_cys_cargabal
		, TRIM(a9600.pdn) cod_cys_plandenegocios
		, TRIM(a9600.em) cod_cys_em
		, TRIM(a9600.cuenta_ant) cod_nro_cuentaanterior
		, TRIM(a9600.rubro_niif) cod_cys_rubroniif
		, IF(SUBSTRING(TRIM(a9600.rubro_bcra),3,1) IN ('5','6'), 'USD', 'ARS') cod_div_divisa
		, NULL cod_nro_cuentaregularizadora
		, h7770.fc_cys_saldomes
		, h7770.fc_cys_saldopromediomes
		, TRIM(REGEXP_REPLACE(a9600.enero, ",", ".")) fc_cys_enero
		, TRIM(REGEXP_REPLACE(a9600.febrero, ",", ".")) fc_cys_febrero
		, TRIM(REGEXP_REPLACE(a9600.marzo, ",", ".")) fc_cys_marzo
		, TRIM(REGEXP_REPLACE(a9600.abril, ",", ".")) fc_cys_abril
		, TRIM(REGEXP_REPLACE(a9600.mayo, ",", ".")) fc_cys_mayo
		, TRIM(REGEXP_REPLACE(a9600.junio, ",", ".")) fc_cys_junio
		, TRIM(REGEXP_REPLACE(a9600.julio, ",", ".")) fc_cys_julio
		, TRIM(REGEXP_REPLACE(a9600.agosto, ",", ".")) fc_cys_agosto
		, TRIM(REGEXP_REPLACE(a9600.septiembre, ",", ".")) fc_cys_septiembre
		, TRIM(REGEXP_REPLACE(a9600.octubre, ",", ".")) fc_cys_octubre
		, TRIM(REGEXP_REPLACE(a9600.noviembre, ",", ".")) fc_cys_noviembre
		, TRIM(REGEXP_REPLACE(a9600.diciembre, ",", ".")) fc_cys_diciembre
		, NULL fc_cys_cierreanioanteriorajustinflacion
		, TRIM(REGEXP_REPLACE(a9601.enero, ",", ".")) fc_cys_eneroajustinflacion
		, TRIM(REGEXP_REPLACE(a9601.febrero, ",", ".")) fc_cys_febreroajustinflacion
		, TRIM(REGEXP_REPLACE(a9601.marzo, ",", ".")) fc_cys_marzoajustinflacion
		, TRIM(REGEXP_REPLACE(a9601.abril, ",", ".")) fc_cys_abrilajustinflacion
		, TRIM(REGEXP_REPLACE(a9601.mayo, ",", ".")) fc_cys_mayoajustinflacion
		, TRIM(REGEXP_REPLACE(a9601.junio, ",", ".")) fc_cys_junioajustinflacion
		, TRIM(REGEXP_REPLACE(a9601.julio, ",", ".")) fc_cys_julioajustinflacion
		, TRIM(REGEXP_REPLACE(a9601.agosto, ",", ".")) fc_cys_agostoajustinflacion
		, TRIM(REGEXP_REPLACE(a9601.septiembre, ",", ".")) fc_cys_septiembreajustinflacion
		, TRIM(REGEXP_REPLACE(a9601.octubre, ",", ".")) fc_cys_octubreajustinflacion
		, TRIM(REGEXP_REPLACE(a9601.noviembre, ",", ".")) fc_cys_noviembreajustinflacion
		, TRIM(REGEXP_REPLACE(a9601.diciembre, ",", ".")) fc_cys_diciembreajustinflacion
		, h7770.partition_date
	FROM hals7770 h7770 
	
	LEFT JOIN bi_corp_staging.malha_alha9600 a9600
		ON TRIM(a9600.rubro_altair) = h7770.cod_cys_rubroaltair
		AND a9600.partition_date = '2021-05-20'
	
	LEFT JOIN bi_corp_staging.malha_alha9601 a9601
		ON TRIM(a9601.rubro_altair) = h7770.cod_cys_rubroaltair
		AND a9601.partition_date = '2021-05-20' ;

	
	
	
	
	
	SELECT DISTINCT TRIM(a9601.rubro_altair) cod_cys_rubroaltair , partition_date
	FROM bi_corp_staging.malha_alha9601 a9601 
	WHERE a9601.partition_date = '2021-05-17' 
	AND TRIM(a9601.rubro_altair) = '235009'

	SELECT DISTINCT TRIM(a9600.rubro_altair) cod_cys_rubroaltair , partition_date
	FROM bi_corp_staging.malha_alha9600 a9600 
	WHERE a9600.partition_date = '2021-05-17' 
	AND TRIM(a9600.rubro_altair) = '235009'
	
	SELECT DISTINCT TRIM(cuenta_contable_pl1) cod_cys_rubroaltair , partition_date
	FROM bi_corp_staging.malha_hals7770 
	WHERE partition_date = '2021-05-17' 
	AND TRIM(cuenta_contable_pl1) = '235009'
	
	
	
	
	