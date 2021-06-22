
--__/__/__/__/__/__/__/__/__/__/__/__/__/__/__/__/__/__/__/__/__/__/__/__/__/__/__/__/__/__/

SELECT * FROM bi_corp_staging.rio44_ba_disponibilidad_atm ;
-- SHOW PARTITIONS bi_corp_staging.rio44_ba_disponibilidad_atm ; --partition_date=2020-10-01

SELECT * FROM bi_corp_staging.rio44_ba_equipos  ;
-- SHOW PARTITIONS bi_corp_staging.rio44_ba_equipos ; --partition_date=2020-11-17

SELECT * FROM bi_corp_staging.rio44_ba_equipos_ba_alta  ;
-- SHOW PARTITIONS bi_corp_staging.rio44_ba_equipos_ba_alta ; --partition_date=2020-04-08

SELECT * FROM bi_corp_staging.rio44_ba_equipos_marca ;
-- SHOW PARTITIONS bi_corp_staging.rio44_ba_equipos_marca ; --partition_date=2020-11-17

SELECT * FROM bi_corp_staging.rio44_ba_equipos_modelo ;
-- SHOW PARTITIONS bi_corp_staging.rio44_ba_equipos_modelo ; --partition_date=2020-11-18

SELECT * FROM bi_corp_staging.rio44_ba_equipos_operador ;
-- SHOW PARTITIONS bi_corp_staging.rio44_ba_equipos_operador ; --partition_date=2020-11-18

SELECT * FROM bi_corp_staging.rio44_ba_equipos_posicion ;
-- SHOW PARTITIONS bi_corp_staging.rio44_ba_equipos_posicion ; --partition_date=2020-11-18

SELECT * FROM bi_corp_staging.rio44_ba_posicion_domicilio ;
-- SHOW PARTITIONS bi_corp_staging.rio44_ba_posicion_domicilio  --partition_date=2020-11-18

SELECT * FROM bi_corp_staging.rio44_ba_posicion_tipo ;
-- SHOW PARTITIONS bi_corp_staging.rio44_ba_posicion_tipo  --partition_date=2020-11-18

SELECT * FROM bi_corp_staging.rio44_ba_posicion_num ;
-- SHOW PARTITIONS bi_corp_staging.rio44_ba_posicion_num --partition_date=2020-11-18

SELECT * FROM bi_corp_staging.rio44_ba_provincia ;
-- SHOW PARTITIONS bi_corp_staging.rio44_ba_provincia --partition_date=2020-11-18

SELECT * FROM bi_corp_staging.rio44_ba_suc_zona ;
-- SHOW PARTITIONS bi_corp_staging.rio44_ba_suc_zona --partition_date=2020-11-18

SELECT * FROM bi_corp_staging.rio44_ba_zonal ;
-- SHOW PARTITIONS bi_corp_staging.rio44_ba_zonal ; --partition_date=2020-11-18

SELECT * FROM bi_corp_staging.rio44_ba_posicion ;
-- SHOW PARTITIONS bi_corp_staging.rio44_ba_posicion ; --partition_date ='2020-11-19'

SELECT * FROM bi_corp_staging.rio44_ba_equipos_modif ;
-- SHOW PARTITIONS bi_corp_staging.rio44_ba_equipos_modif ; --partition_date ='2020-11-19'

-------------------------------------------------------------------------------------------

SELECT * FROM bi_corp_staging.rio44_ba_disponibilidad_mensual  ;
-- SHOW PARTITIONS bi_corp_staging.rio44_ba_disponibilidad_mensual ; --empty

SELECT * FROM bi_corp_staging.rio44_ba_equipos_dispo_tas_cerrado ;
-- SHOW PARTITIONS bi_corp_staging.rio44_ba_equipos_dispo_tas_cerrado ; --empty

SELECT * FROM bi_corp_staging.rio44_ba_equipos_dispo_atm_men_tb  ;
-- SHOW PARTITIONS bi_corp_staging.rio44_ba_equipos_dispo_atm_men_tb ; --empty

SELECT * FROM bi_corp_staging.rio44_ba_equipos_dispo_tas  ;
-- SHOW PARTITIONS bi_corp_staging.rio44_ba_equipos_dispo_tas ; --empty


DESCRIBE bi_corp_staging.rio44_ba_disponibilidad_atm ;
DESCRIBE bi_corp_staging.rio44_ba_disponibilidad_mensual  ;
DESCRIBE bi_corp_staging.rio44_ba_equipos  ;
DESCRIBE bi_corp_staging.rio44_ba_equipos_dispo_atm_men_tb  ;
DESCRIBE bi_corp_staging.rio44_ba_equipos_dispo_tas  ;
DESCRIBE bi_corp_staging.rio44_ba_equipos_dispo_tas_cerrado ;
DESCRIBE bi_corp_staging.rio44_ba_equipos_marca ;
DESCRIBE bi_corp_staging.rio44_ba_equipos_modelo ;
DESCRIBE bi_corp_staging.rio44_ba_equipos_modif ;
DESCRIBE bi_corp_staging.rio44_ba_equipos_operador ;
DESCRIBE  bi_corp_staging.rio44_ba_equipos_posicion ;
DESCRIBE bi_corp_staging.rio44_ba_posicion_domicilio ;
DESCRIBE bi_corp_staging.rio44_ba_posicion ;
DESCRIBE bi_corp_staging.rio44_ba_posicion_tipo ;
DESCRIBE bi_corp_staging.rio44_ba_posicion_num ;
DESCRIBE  bi_corp_staging.rio44_ba_provincia ;
DESCRIBE bi_corp_staging.rio44_ba_suc_zona ;
DESCRIBE bi_corp_staging.rio44_ba_zonal ;


SELECT * FROM bi_corp_staging.rio30_transacciones WHERE partition_date = '2020-11-19' ;

-----

-- 68.678 total
-- 
SELECT COUNT(1) FROM bi_corp_staging.rio151_tbl_interaccion WHERE partition_date = '2020-11-18' AND cd_canal_venta = 'TK' OR cd_canal_venta IS NULL ;
-- AC,BC,CE,CO,ER,FP,FV,SP,SU,TK,UG
SELECT DISTINCT cd_canal_venta FROM bi_corp_staging.rio151_tbl_interaccion WHERE partition_date = '2020-11-18' ;

--1,415
SELECT COUNT(1) FROM bi_corp_common.bt_cc_mascheinteraccion WHERE partition_date = '2020-11-18' ;
-- TELEMARK.
SELECT DISTINCT ds_cc_canalventa FROM bi_corp_common.bt_cc_mascheinteraccion WHERE partition_date = '2020-11-18' ;

SELECT * FROM bi_corp_common.bt_cc_mascheinteraccion WHERE partition_date = '2020-11-18' 
AND cod_cc_interaccionpadre IS NOT null;



-- VTADIR01.BA_EQUIPOS_BA_ALTA source

-- CREATE OR REPLACE VIEW VTADIR01.BA_EQUIPOS_BA_ALTA (
-- ID_EQUIPOS, SIGLA, NRO_SERIE, EQUIPO, MODELO_DESCRI, MARCA_DESCRI, APERTURA, RECONOC_BILL, FUNCIONALIDAD, POSICION_NUM, POSICION_NUM_CC, ID_ZONA, DESCRI_ZONAL,
-- AREA, POSICION_NOM, DESCRI_POS_TIPO, CALLE, NUMERO, POSTAL, POSTAL_CPA, TEL, LOCALIDAD, PROVINCIA, F_MODIF, OPERADOR_DESCRIP, RED) AS

-- COUNT: 6.118 all | filter id_sed: 2.905 | (sin JOIN) --
SELECT COUNT(1) FROM ( 
WITH pos_mod AS (
SELECT id_posicion, MAX(f_modif) f_modif FROM bi_corp_staging.rio44_ba_equipos_modif 
WHERE partition_date = '2020-11-19' GROUP BY id_posicion )

SELECT TRIM(xe.id_equipos) id_equipos
	, TRIM(xe.sigla) sigla
	, TRIM(xe.nro_serie) nro_serie
	, TRIM(xe.equipo) equipo
	, TRIM(em.modelo_descri) modelo_descri
	, TRIM(ema.marca_descri) marca_descri
	, TRIM(xe.apertura) apertura 
	, TRIM(xe.reconoc_bill) reconoc_bill 
	, TRIM(xe.funcionalidad) funcionalidad 
	, TRIM(pnum.posicion_num) ba_posicion_num 
	--, pnum2.posicion_num posicion_num_CC
	--, z.zona_nro id_zona
	--, z.descri_zonal
	--, z.area 
	, TRIM(p.nombre) posicion_nom
	--, pt.descri_pos_tipo
	--, nvl(pd.calle,' ') calle 
	--, nvl(pd.numero,' ') numero
	--, nvl(to_char(pd.postal),	' ') postal
	--, nvl(pd.postal_cpa,	' ') postal_cpa
	--, nvl(pd.tel,	' ') tel
	--, nvl(rvd_l.descripcion,	' ') Localidad
	--, nvl(prov.descripcion,	' ') provincia 
	, pos_mod.f_modif
	, TRIM(eo.operador_descrip) operador_descrip
	--, fx_ba_red_equipo(xe.sigla) red
	----------------------------------
	, TRIM(xe.modelo) modelo
	, TRIM(xe.operador) operador
	, TRIM(xe.id_sed) id_sed 
	, xe.partition_date
FROM bi_corp_staging.rio44_ba_equipos xe
	LEFT JOIN bi_corp_staging.rio44_ba_equipos_modelo em -- ba_equipos_modelo em,
		ON TRIM(xe.modelo) = TRIM(em.id_modelo)
		AND em.partition_date = xe.partition_date 
	LEFT JOIN bi_corp_staging.rio44_ba_equipos_marca ema -- ba_equipos_marca ema,
		ON TRIM(em.marca_id) = TRIM(ema.id_marca)
		AND ema.partition_date = xe.partition_date
	LEFT JOIN bi_corp_staging.rio44_ba_equipos_posicion ep -- ba_equipos_posicion ep,
		ON TRIM(xe.id_equipos) = TRIM(ep.id_equipos)
		AND ep.f_baja != 'null'
		AND ep.partition_date = xe.partition_date
	LEFT JOIN bi_corp_staging.rio44_ba_posicion p -- ba_posicion p,
		ON TRIM(ep.id_posicion) = TRIM(p.id_posicion)
		AND p.partition_date = xe.partition_date
	LEFT JOIN bi_corp_staging.rio44_ba_equipos_operador eo -- ba_equipos_operador eo,
		ON trim(xe.operador) = trim(eo.id_operador)
		AND eo.partition_date = xe.partition_date
	LEFT JOIN bi_corp_staging.rio44_ba_posicion_num pnum -- ba_posicion_num pnum,
		ON TRIM(p.numero_id) = TRIM(pnum.id_posicion_num)
		AND pnum.partition_date = xe.partition_date
	LEFT JOIN pos_mod pos_mod
		ON p.id_posicion = pos_mod.id_posicion
	-- ba_zonal z
	-- ba_posicion_num pnum2,
	LEFT JOIN bi_corp_staging.rio44_ba_posicion_num pnum2
		ON TRIM(p.numero_id) = TRIM(pnum.id_posicion_num)
		AND pnum.partition_date = xe.partition_date
	-- ba_posicion_domicilio pd,
	-- ba_provincia prov ,
	-- ba_posicion_tipo pt,
	-- rvd.rpt_d_localidad rvd_l,
	-- ba_suc_zona sz,
	
WHERE xe.partition_date = '2020-11-24'
	AND TRIM(xe.id_sed) IN ('7','56','55')
) A ;

----------------------------------------------
	AND pnum2.posicion_num = sz.suc (+)
	AND sz.zona = z.id_zona (+)
	AND p.numero_id_cont = pnum2.id_posicion_num (+)
	AND p.id_posicion = pd.posicion_id
	AND pd.provincia_id = prov.id_provincia (+)
	AND p.posicion_tipo_id = pt.id_posicion_tipo (+)
	AND pd.localidad_id = rvd_l.localidadsid (+)

SHOW PARTITIONS bi_corp_staging.rio44_ba_posicion

SELECT * FROM bi_corp_staging.rio44_ba_equipos_posicion WHERE partition_date = '2020-11-24'




SELECT * FROM bi_corp_staging.rio301_reneg_offer WHERE partition_date = '2020-11-27' ;

SELECT * FROM bi_corp_staging.rio157_ms0_ft_dwh_blce_result_dia 


SELECT date_format('2017-07-01','dd-MM-yyyy')


SELECT * FROM bi_corp_common.bt_ror_input_activo 
WHERE partition_date = '2020-10-31' LIMIT 10 ;


-- 386.018.313
SELECT COUNT(1) FROM bi_corp_common.bt_ror_input_activo 
WHERE partition_date = '2020-10-31' ;


-- COM00720000020001000001109928000	00720000020001000001109928000
-- COM TOTAL: 30.405.578
-- COM PASIVO EN ACTIVO: 2.982.197
SELECT COUNT(1) FROM bi_corp_common.bt_ror_input_activo 
WHERE partition_date = '2020-10-31' 
AND SUBSTRING(cod_ren_contrato_rorac, 1, 3) = 'COM' AND SUBSTRING(cod_productogest, 1, 2) = 'C-'
--AND cod_ren_contrato = '00720000020001000001109928000'


SELECT SUBSTRING(cod_ren_contrato_rorac, 1, 3) contenido , SUBSTRING(cod_productogest, 1, 2) pg, COUNT(1) q
FROM bi_corp_common.bt_ror_input_activo 
WHERE partition_date = '2020-10-31' 
AND SUBSTRING(cod_productogest, 1, 2) IN ('C-', 'C+')
GROUP BY SUBSTRING(cod_ren_contrato_rorac, 1, 3), SUBSTRING(cod_productogest, 1, 2) ;


SELECT SUBSTRING(cod_productogest, 1, 2) , COUNT(1) FROM bi_corp_common.bt_ror_input_activo 
WHERE partition_date = '2020-10-31' 
AND SUBSTRING(cod_ren_contrato_rorac, 1, 3) = 'CCL' -- AND cod_ren_contrato != '-99100'
GROUP BY SUBSTRING(cod_productogest, 1, 2)


-- COM ACTIVO EN PASIVO: 27.311.319
SELECT COUNT(1) FROM bi_corp_common.bt_ror_input_pasivo 
WHERE partition_date = '2020-10-31' 
AND SUBSTRING(cod_ren_contrato_rorac, 1, 3) = 'COM' AND SUBSTRING(cod_productogest, 1, 2) = 'C+'

SELECT cod_ren_contrato , SUBSTRING(cod_ren_contrato_rorac, 1, 3) contenido
FROM bi_corp_common.bt_ror_input_pasivo 
WHERE partition_date = '2020-10-31' 
AND cod_ren_contrato != '-99100' -- ;
GROUP BY cod_ren_contrato , SUBSTRING(cod_ren_contrato_rorac, 1, 3) HAVING COUNT(cod_ren_contrato) > 1 ;

WITH contratos_com AS 
(SELECT DISTINCT cod_ren_contrato FROM bi_corp_common.bt_ror_input_pasivo 
WHERE partition_date = '2020-10-31' AND SUBSTRING(cod_ren_contrato_rorac, 1, 3) = 'COM')

SELECT DISTINCT re.cod_ren_contrato , SUBSTRING(re.cod_ren_contrato_rorac, 1, 3) contenido
FROM bi_corp_common.bt_ror_input_pasivo re
JOIN contratos_com com ON re.cod_ren_contrato = com.cod_ren_contrato
WHERE re.partition_date = '2020-10-31' 
AND SUBSTRING(cod_ren_contrato_rorac, 1, 3) IN ('PRE','CRE','ARF','CCO') ;


AND  cod_ren_contrato = '00720000010001090005055400016' 
;

AND SUBSTRING(cod_ren_contrato_rorac, 1, 3) = 'COM' AND SUBSTRING(cod_productogest, 1, 2) = 'C+'



SELECT partition_date, count(*)
FROM bi_corp_common.bt_ga_canales
WHERE partition_date >= '2020-11-26'
GROUP BY partition_date ;

-- -83447.6264 | SUBSTRING(cod_ren_producto_gest, 1, 2) = 'C+' | 

SELECT dt_ren_fechadata, cod_ren_contrato, cod_ren_area_negocio
	-- ,   
	, sum(fc_ren_resultado_total_real_ml) ComNoFin                                                                                                                                         
FROM bi_corp_common.bt_ren_result  --BWHCORE.MS0_FT_DWH_BLCE_RESULT subpartition (PMES_54_202010_CCL) 
WHERE partition_date = '2020-10-31'                                                                                                                                           
	AND cod_ren_contenido = 'CCL'  -- BWHCORE.MS0_FT_DWH_BLCE_RESULT.COD_CONTENIDO = 'CCL'                                                                                 
     -- BWHCORE.MS0_FT_DWH_BLCE_RESULT.FEC_DATA = 20201031      AND                                                                                      
    AND cod_ren_contrato = '-99100' -- BWHCORE.MS0_FT_DWH_BLCE_RESULT.IDF_CTO_ODS  like '-99100%'  AND 
    AND cod_ren_pers_ods != '-99100' -- BWHCORE.MS0_FT_DWH_BLCE_RESULT.IDF_PERS_ODS <> '-99100      ' AND
    AND SUBSTRING(cod_ren_producto_gest, 1, 2) = 'C+'  -- BWHCORE.MS0_FT_DWH_BLCE_RESULT.COD_PRODUCTO_GEST like  'C+%'  
    AND TRIM(cod_ren_area_negocio) = 'P2AGRCIT'
GROUP BY dt_ren_fechadata, cod_ren_contrato, cod_ren_area_negocio ;



SELECT DISTINCT cod_ren_producto_niv_3 
FROM bi_corp_common.bt_ren_result  
WHERE partition_date = '2020-10-31'                                                                                                                                           
	AND cod_ren_contenido = 'COM' ;
-- CCL
-- -----------------------
-- cod_ren_producto_niv_3|
-- ----------------------|
-- PG909999              |
-- BG905                 |
-- BG1                   |
-- BG2                   |
-- BG3                   |
-- BG90                  |



SELECT * FROM bi_corp_common.bt_ror_input_activo 
WHERE partition_date = '2020-09-30' 
AND 

AND SUBSTRING(cod_ren_contrato_rorac, 1, 3) = 'COM'

SELECT * FROM bi_corp_common.bt_ren_result WHERE partition_date = '2020-09-30' 
AND cod_ren_contrato = '00720000020001000004005544000' AND cod_ren_contenido IN 'COM'


SELECT partition_date , count(1) registros
FROM bi_corp_common.bt_ren_result_dia 
WHERE partition_date >= '2020-11-30'
GROUP BY partition_date ;



/*
20201211
20201210
20201209
20201204
20201203
20201202
20201201
*/

SELECT * FROM bi_corp_staging.garra_log_cuotaphone LIMIT 10 ;


SELECT * FROM bi_corp_staging.nesr_encolador_det_ticket 
