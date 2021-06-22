-- cod_contenido|count      |
----------------|-----------|
-- PRE          |221.574.983|
-- CCL          |119.489.977|
-- CRE          | 61.444.791|
-- CTA          | 49.610.452|
-- COM          | 25.260.338|
-- PLZ          | 16.919.764|
-- ROF          |  3.782.428|
-- CTB          |  1.371.624|
-- CCO          |  1.317.796|
-- INS          |  1.176.591|
-- FON          |  1.048.688|
-- GAS          |    356.794|
-- ARF          |     14.267|
-- CTG          |      1.227|	
--------------------------------------------------------------------------------

--     ____    ___       ____                           
--    / __ \  /   |     / __ \ ____   _____ ____ _ _____
--   / / / / / /| |    / /_/ // __ \ / ___// __ `// ___/
--  / /_/ / / ___ |   / _, _// /_/ // /   / /_/ // /__  
--  \___\_\/_/  |_|  /_/ |_| \____//_/    \__,_/ \___/  
--                                                      

--------------------------------------------------------------------------------

-- Diferencias pasivos:
SELECT * FROM bi_corp_common.bt_ror_input_pasivo_v2 
WHERE cod_ren_contrato_rorac = 'PLZ00720815010016000000175100001' 

SELECT * FROM bi_corp_common.bt_ren_rorac_input_pasivo
WHERE cod_ren_contrato_rorac = 'PLZ00720815010016000000175100001'

SELECT * FROM bi_corp_common.bt_ren_result_test 
WHERE cod_ren_contrato_rorac = 'PLZ00720815010016000000175100001'

--
SELECT fc_ren_margenmes,fc_ren_sdbsmv,fc_ren_sfbsmv , 'bt_ren_rorac_input_pasivo' origen
FROM bi_corp_common.bt_ren_rorac_input_pasivo
WHERE cod_ren_contrato_rorac = 'PLZ00720815010016000000175100001'
UNION ALL 
SELECT fc_ren_margenmes,fc_ren_sdbsmv,fc_ren_sfbsmv , 'bt_ror_input_pasivo_v2' origen
FROM bi_corp_common.bt_ror_input_pasivo_v2 
WHERE cod_ren_contrato_rorac = 'PLZ00720815010016000000175100001'


-- Tablas para control:
-- contenidos: ('PRE','CRE','ARF','CCO','COM','CCL')
SELECT * FROM bi_corp_common.bt_ror_input_activo_v2 ;

-- contenidos: ('PLZ','CTA','COM','CCL')
SELECT * FROM bi_corp_common.bt_ror_input_pasivo_v2 ;

-------------------------------------------------------------
-- TOTALES:
-- SELECT COUNT(1) FROM bi_corp_common.bt_ror_input_activo_v2 ;     -- 74.391.282 -- 50.283.610
-- SELECT COUNT(1) FROM bi_corp_common.bt_ren_rorac_input_activo ;  -- 74.391.282 
-- SELECT COUNT(1) FROM bi_corp_common.bt_ror_input_pasivo_v2 ;     -- 18.905.945 -- 22.011.886
-- SELECT COUNT(1) FROM bi_corp_common.bt_ren_rorac_input_pasivo ;  -- 18.905.948

SELECT COUNT(1) FROM bi_corp_common.bt_ren_rorac_input_activo 
WHERE cod_ren_areanegocio NOT IN ('998','999') ; -- 39.897.989

SELECT COUNT(1) FROM bi_corp_common.bt_ror_input_activo_v2 
WHERE cod_nivel_operacion = 0 
AND SUBSTRING(cod_ren_contrato_rorac,1,3) != 'COM'; -- 39.897.989 

-------------------------------------------------------------------------

SELECT a.cod_ren_contrato_rorac , b.cod_ren_contrato_rorac 
FROM bi_corp_common.bt_ren_rorac_input_activo a
LEFT JOIN bi_corp_common.bt_ror_input_activo_v2 b
	ON a.cod_ren_contrato_rorac = b.cod_ren_contrato_rorac 
	AND b.cod_nivel_operacion = 0 
	AND SUBSTRING(b.cod_ren_contrato_rorac,1,3) != 'COM'
WHERE a.cod_ren_areanegocio != '998' -- 39.897.989 
	AND b.cod_ren_contrato_rorac IS NULL;

-------------------------------------------------------------------------------------------------

-- CTA00720250070001000003609808001
-- CTA00720250070001000003609808001
-- CTA00720250070001000003609808001

SELECT *  
FROM bi_corp_common.bt_ren_rorac_input_pasivo
WHERE cod_ren_contrato_rorac = 'CTA00720250070001000003609808001'

SELECT a.cod_ren_contrato_rorac , b.cod_ren_contrato_rorac  
FROM bi_corp_common.bt_ren_rorac_input_pasivo a 
LEFT JOIN bi_corp_common.bt_ror_input_pasivo_v2 b 
ON a.cod_ren_contrato_rorac = b.cod_ren_contrato_rorac 
WHERE b.cod_ren_contrato_rorac IS NULL; 

SELECT DISTINCT SUBSTRING(cod_ren_contrato_rorac,1,3) FROM bi_corp_common.bt_ror_input_pasivo_v2
WHERE cod_nivel_operacion = 1 ;

-------------------------------------------------------------------------------------------------

-- 20200831: add -- insolvencias:
-- SELECT * FROM santander_business_risk_ifrs9.ifrs9_tablon 
-- entidad - centro - prod - subprodu 

SHOW PARTITIONS santander_business_risk_ifrs9.ifrs9_tablon ;
DESCRIBE santander_business_risk_ifrs9.ifrs9_tablon ;


-- t_idf_cto_ifrs = '0072_0000_000000081773_60_0000_ARS'
-- t_cod_entidad          + '_'
-- t_cod_centro           + '_'
-- t_num_cuenta           + '_'
-- t_cod_producto         + '_'
-- t_cod_subprodu_altair  + '_'
-- t_cod_divisa           
SELECT *
FROM santander_business_risk_ifrs9.ifrs9_tablon  
WHERE periodo = '20200630' 
AND t_num_cuenta = '000000000000'
--AND t_cod_entidad = '0072' AND t_cod_centro = '0013' AND t_cod_producto = '70'
--AND t_cod_subprodu_altair = 'C070' AND t_cod_divisa = 'ARS'     
AND t_idf_cto_ifrs = '0072_0013_000002098063_70_C070_ARS';

-- 0072_0000_100004617913_41_INT_ARS 

----------------------------------------------------------------------------------------------
-- 998,999
SELECT DISTINCT cod_area_negocio 
FROM bi_corp_staging.rio157_ms0_ft_dwh_blce_result 
WHERE partition_date = '2020-06-30'
AND ind_pool = 'S'
----------------------------------------------------------------------------------------------
-- '0072_0013_000002098063_70_C070_ARS'
-- CONTRATO IFRS9: 
SELECT concat_ws('_'
			, SUBSTRING(cod_ren_contrato,1,4)   -- t_cod_entidad
			, SUBSTRING(cod_ren_contrato,5,4)   -- t_cod_centro 
			, SUBSTRING(cod_ren_contrato,15,12) -- t_num_cuenta
			, SUBSTRING(cod_ren_contrato,9,2)   -- t_cod_producto 
			, SUBSTRING(cod_ren_contrato,11,4)  -- t_cod_subprodu_altair
			, cod_ren_divisa )
		, cod_ren_contrato_rorac 
FROM bi_corp_common.bt_ror_input_activo_v2
WHERE cod_nivel_operacion = 0
AND cod_ren_contrato_rorac = 'INS0072001370C070000002098063000';



-----------------------------------------------------------------------------------------------
-- ##########################################################################################--
--       _  ____             ____ 
--      (_)/ __/_____ _____ / __ \
--     / // /_ / ___// ___// /_/ /
--    / // __// /   (__  ) \__, / 
--   /_//_/  /_/   /____/ /____/  
-- 
-- DROP TABLE tablon_ifrs9 ;
CREATE TEMPORARY TABLE IF NOT EXISTS tablon_ifrs9 AS  
SELECT CONCAT(RI.t_cod_entidad, RI.t_cod_centro, RI.t_cod_producto, lpad(RI.t_cod_subprodu_altair,4,'0'), RI.t_num_cuenta) cod_ren_contrato_short
	, RI.t_idf_cto_ifrs
	, RI.t_cod_entidad
	, RI.t_cod_centro
	, RI.t_num_cuenta
	, RI.t_cod_producto
	, lpad(RI.t_cod_subprodu_altair,4,'0') t_cod_subprodu_altair
	, RI.t_cod_divisa
	, SUM(RI.s_provision_amount) s_provision_amount
FROM santander_business_risk_ifrs9.ifrs9_tablon RI
WHERE RI.periodo = '20200630'
	AND RI.t_cod_divisa = 'ARS'
	AND RI.t_cod_entidad IS NOT NULL
	AND RI.t_cod_centro IS NOT NULL
	AND RI.t_cod_producto IS NOT NULL
	AND RI.t_cod_subprodu_altair IS NOT NULL
	AND RI.t_cod_divisa IS NOT NULL
GROUP BY RI.t_idf_cto_ifrs
	, RI.t_cod_entidad
	, RI.t_cod_centro
	, RI.t_num_cuenta
	, RI.t_cod_producto
	, lpad(RI.t_cod_subprodu_altair,4,'0') 
	, RI.t_cod_divisa ;


-------------------------------------------------------------------------
CREATE TEMPORARY TABLE IF NOT EXISTS input_activo_prov AS 
SELECT RE.* 
	, RI.s_provision_amount AS fc_ifrs9_provision
FROM bi_corp_common.bt_ror_input_activo_v2 RE
LEFT JOIN tablon_ifrs9 RI ON 
	SUBSTRING(RE.cod_ren_contrato,1,26) = RI.cod_ren_contrato_short
WHERE RE.partition_date = '2020-06-30' ;

--------------------------------------------------------------------------

SELECT SUBSTRING(cod_ren_contrato_rorac,1,3) contenido
	, IF(fc_ifrs_provision IS NOT NULL,1,0) is_provision
	, COUNT(1) registros
FROM bi_corp_common.bt_ror_input_activo_v2 
GROUP BY SUBSTRING(cod_ren_contrato_rorac,1,3) 
	, IF(fc_ifrs_provision IS NOT NULL,1,0) ;

SELECT SUBSTRING(cod_ren_contrato_rorac,1,3) contenido
	, IF(fc_ifrs_provision IS NOT NULL,1,0) is_provision
	, COUNT(1) registros
FROM bi_corp_common.bt_ror_input_pasivo_v2 
GROUP BY SUBSTRING(cod_ren_contrato_rorac,1,3) 
	, IF(fc_ifrs_provision IS NOT NULL,1,0) ;

--------------------------------------------------------------------------


SELECT COUNT(1) FROM input_activo_prov ; -- 50.993.231
SELECT COUNT(1) FROM bi_corp_common.bt_ror_input_pasivo_v2 
WHERE fc_ifrs_provision IS NOT NULL; -- 22.260.766 -- 22.260.766

SELECT COUNT(1) FROM input_activo_prov
WHERE fc_ifrs9_provision IS NOT NULL;

SELECT SUBSTRING(RE.cod_ren_contrato,1,26) 
FROM bi_corp_common.bt_ror_input_activo_v2 RE 
WHERE SUBSTRING(RE.cod_ren_contrato,1,26) != '-99100' 
GROUP BY SUBSTRING(RE.cod_ren_contrato,1,26) 
HAVING COUNT(SUBSTRING(RE.cod_ren_contrato,1,26))>1;	

SELECT * FROM bi_corp_common.bt_ror_input_activo_v2
WHERE SUBSTRING(cod_ren_contrato,1,26) = '00720000210012007000100991'

SELECT cod_ren_contrato_rorac 
FROM input_activo_prov
WHERE fc_ifrs9_provision IS NOT NULL
GROUP BY cod_ren_contrato_rorac 
HAVING COUNT(cod_ren_contrato_rorac )>1;

SELECT cod_ren_contrato_rorac 
FROM input_activo_prov
WHERE cod_ren_contrato_rorac = 'ARF00720124291130029010000584000'
--cod_ren_contrato_rorac = 'CCO00720000210012007000100991000'
----------------------------------------------------
-- ENTIDAD:
-- DISTINCT RI.t_cod_entidad: NULL,'0072'

-- CENTRO:
-- DISTINCT RI.t_cod_centro: NULL , *

-- PRODUCTO:
-- DISTINCT RI.t_cod_producto: NULL , *

-- SUBPRODUCTO: lpad(RI.t_cod_subprodu_altair,4,'0')
-- DISTINCT RI.t_cod_subprodu_altair: *

-- DIVISA:
-- DISTINCT RI.t_cod_divisa: NULL,ARS,CHF,EUR,GBP,USD




SELECT * 
FROM bi_corp_common.bt_ror_input_activo_v2 RE
WHERE RE.partition_date = '2020-06-30'
AND SUBSTRING(RE.cod_ren_contrato,9,2) = '';

SELECT *
FROM santander_business_risk_ifrs9.ifrs9_tablon RI
WHERE RI.periodo = '20200630' 
AND RI.t_cod_subprodu_altair IS NULL LIMIT 10;

-- santander_business_risk_arda.normaliz_productos 
-- cod_aplicativo_fuente = COD_SIS_ORIGEN 
-- and (cod_producto_operac =  cod_producto 
-- and cod_subprodu = cod_ren_subprodu), rescatar cod_subprodu_operac.
-- t_idf_cto_ifrs = '0072_0000_000000081773_60_0000_ARS'



SELECT cod_aplicativo_fuente 
	 , cod_producto 
	 , LENGTH(cod_subprodu_operac) 
	 , cod_subprodu_operac 
	 , cod_subprodu 
	 , IF(SUBSTRING(cod_subprodu,1,1)='0',SUBSTRING(cod_subprodu,2,3),cod_subprodu) subprodu_fx
	 , IF(cod_subprodu_operac = 
	 	IF(SUBSTRING(cod_subprodu,1,1)='0',SUBSTRING(cod_subprodu,2,3),cod_subprodu)
	 	,1,0) flag_OK
FROM santander_business_risk_arda.normaliz_productos
WHERE data_date_part = '20190731'
AND cod_subprodu_operac != cod_subprodu 
AND IF(cod_subprodu_operac = 
	 	IF(SUBSTRING(cod_subprodu,1,1)='0',SUBSTRING(cod_subprodu,2,3),cod_subprodu)
	 	,1,0) = 0 
ORDER BY LENGTH(cod_subprodu_operac) ASC;	



SELECT SUBSTRING(cod_ren_contrato,1,26), COUNT(DISTINCT SUBSTRING(cod_ren_contrato,26,3))
FROM (
SELECT cod_ren_contrato
FROM input_activo_prov
WHERE fc_ifrs9_provision IS NOT NULL
GROUP BY cod_ren_contrato
HAVING COUNT(cod_ren_contrato )>1 )A
GROUP BY SUBSTRING(cod_ren_contrato,1,26)
HAVING COUNT(DISTINCT SUBSTRING(cod_ren_contrato,26,3)) > 1;

-- 27 contratos.
SELECT * FROM bi_corp_common.bt_ror_input_activo_v2
WHERE SUBSTRING(cod_ren_contrato,1,26) = '00720005400INT100129101411' ;



SELECT  COUNT(1) 
FROM bi_corp_staging.rio157_ms0_ft_dwh_blce_result_dia 
WHERE partition_date = '2020-07-31'
GROUP BY partition_date ;


SELECT MIN(cod_ren_contrato) 
FROM bi_corp_common.bt_ror_input_activo_v2
WHERE SUBSTRING(cod_ren_contrato,1,26) = '00720005400INT100129101411' ;

SELECT * 
FROM bi_corp_common.bt_ror_input_activo_v2
WHERE cod_ren_contrato = '00720005400INT100129101411001'


SELECT *
FROM bi_corp_common.bt_ror_input_activo_v2
WHERE SUBSTRING(cod_ren_contrato,1,26) = '00720005400INT100129101411'

SELECT * FROM bi_corp_common.bt_wts_dialogoswatson LIMIT 10 ;



SHOW PARTITIONS bi_corp_common.bt_ren_result_dia 
