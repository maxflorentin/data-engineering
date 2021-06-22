set mapred.job.queue.name=root.dataeng;
set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=4000000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.auto.convert.join=true;
set hive.auto.convert.join.noconditionaltask=true;
set hive.auto.convert.join.noconditionaltask.size=40000000;
--   __                             __          __     __           
--  / /_ ___   ____ ___   ____     / /_ ____ _ / /_   / /___   _____
-- / __// _ \ / __ `__ \ / __ \   / __// __ `// __ \ / // _ \ / ___/
--/ /_ /  __// / / / / // /_/ /  / /_ / /_/ // /_/ // //  __/(__  ) 
--\__/ \___//_/ /_/ /_// .___/   \__/ \__,_//_.___//_/ \___//____/  
--                    /_/                                           

-- GENERIC RESULT -----------------------------------------------------------------------------
CREATE TEMPORARY TABLE generic_result AS
SELECT DISTINCT idf_cto_ods
	, cod_contenido
	, cod_pais
	, from_unixtime(unix_timestamp(fec_alta_cto , 'yyyyMMdd'), 'yyyy-MM-dd') fec_alta_cto
	, from_unixtime(unix_timestamp(fec_ven, 'yyyyMMdd'), 'yyyy-MM-dd') fec_ven
	, from_unixtime(unix_timestamp(fec_reestruc, 'yyyyMMdd'), 'yyyy-MM-dd') fec_reestruc
	, IF(cod_producto = 'null', null, cod_producto) cod_producto
	, IF(cod_subprodu = 'null', null, cod_subprodu) cod_subprodu
FROM bi_corp_staging.rio157_ms0_dt_dwh_generic_result 
WHERE partition_date = '2020-06-30' ;

-- CLIENTE RESULT ------------------------------------------------------------------------------
CREATE TEMPORARY TABLE cliente_result AS
SELECT DISTINCT idf_pers_ods
	, CAST(SUBSTR(idf_pers_ods, 6, 8) AS INT) per_nup
	, cod_vincula
	, cod_tip_persona
	, CONCAT(IF(nom_nombre = 'null', '', nom_nombre),' ',IF(nom_apellido_1 = 'null', '', nom_apellido_1)) nombre_cliente
FROM bi_corp_staging.rio157_ms0_dt_dwh_cliente_result
WHERE partition_date = '2020-06-30' ;

-----------------------------------------------------------------------------------------------

-- RESULT + PROD + ADN + CTA RESULT 
set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=4000000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.auto.convert.join=true;
set hive.auto.convert.join.noconditionaltask=true;
set hive.auto.convert.join.noconditionaltask.size=40000000;
set mapred.job.queue.name=root.dataeng;

CREATE TEMPORARY TABLE result_je AS
SELECT CONCAT(RE.cod_contenido,RE.idf_cto_ods) AS cod_ren_contrato_rorac ,
	CONCAT(RE.cod_contenido, RE.idf_pers_ods) cod_ren_cliente_rorac ,
	RE.* ,
	adn.cod_ren_area_negocio_niv_1,
	adn.cod_ren_area_negocio_niv_2,
	adn.cod_ren_area_negocio_niv_3,
	adn.cod_ren_area_negocio_niv_4,
	adn.cod_ren_area_negocio_niv_5,
	adn.cod_ren_area_negocio_niv_6,
	adn.cod_ren_area_negocio_niv_7,
	adn.cod_ren_area_negocio_niv_8,
	adn.cod_ren_area_negocio_niv_9,
	adn.ds_ren_area_negocio_niv_1,
	adn.ds_ren_area_negocio_niv_2,
	adn.ds_ren_area_negocio_niv_3,
	adn.ds_ren_area_negocio_niv_4,
	adn.ds_ren_area_negocio_niv_5,
	adn.ds_ren_area_negocio_niv_6,
	adn.ds_ren_area_negocio_niv_7,
	adn.ds_ren_area_negocio_niv_8,
	adn.ds_ren_area_negocio_niv_9,
	prod.cod_ren_producto_niv_1,
	prod.cod_ren_producto_niv_2,
	prod.cod_ren_producto_niv_3,
	prod.cod_ren_producto_niv_4,
	prod.cod_ren_producto_niv_5,
	prod.cod_ren_producto_niv_6,
	prod.cod_ren_producto_niv_7,
	prod.cod_ren_producto_niv_8,
	prod.cod_ren_producto_niv_9,
	prod.cod_ren_producto_niv_10,
	prod.ds_ren_producto_niv_1,
	prod.ds_ren_producto_niv_2,
	prod.ds_ren_producto_niv_3,
	prod.ds_ren_producto_niv_4,
	prod.ds_ren_producto_niv_5,
	prod.ds_ren_producto_niv_6,
	prod.ds_ren_producto_niv_7,
	prod.ds_ren_producto_niv_8,
	prod.ds_ren_producto_niv_9,
	prod.ds_ren_producto_niv_10,
	cta.cod_ren_cta_resultados_niv_1,
	cta.cod_ren_cta_resultados_niv_2,
	cta.cod_ren_cta_resultados_niv_3,
	cta.cod_ren_cta_resultados_niv_4,
	cta.cod_ren_cta_resultados_niv_5,
	cta.cod_ren_cta_resultados_niv_6,
	cta.cod_ren_cta_resultados_niv_7,
	cta.cod_ren_cta_resultados_niv_8,
	cta.cod_ren_cta_resultados_niv_9,
	cta.cod_ren_cta_resultados_niv_10,
	cta.cod_ren_cta_resultados_niv_11,
	cta.cod_ren_cta_resultados_niv_12,
	cta.ds_ren_cta_resultados_niv_1,
	cta.ds_ren_cta_resultados_niv_2,
	cta.ds_ren_cta_resultados_niv_3,
	cta.ds_ren_cta_resultados_niv_4,
	cta.ds_ren_cta_resultados_niv_5,
	cta.ds_ren_cta_resultados_niv_6,
	cta.ds_ren_cta_resultados_niv_7,
	cta.ds_ren_cta_resultados_niv_8,
	cta.ds_ren_cta_resultados_niv_9,
	cta.ds_ren_cta_resultados_niv_10,
	cta.ds_ren_cta_resultados_niv_11,
	cta.ds_ren_cta_resultados_niv_12,
	entj.des_hijo
FROM bi_corp_staging.rio157_ms0_ft_dwh_blce_result RE 
JOIN bi_corp_common.dim_ren_productosctrldn prod ON
	RE.cod_producto_gest = prod.cod_ren_producto 
JOIN bi_corp_common.dim_ren_areanegocioctr adn ON 
	RE.cod_area_negocio = adn.cod_ren_area_negocio 
JOIN bi_corp_staging.rio157_ms0_dm_je_dwh_entidades_ctr entj ON 
	entj.partition_date = '2020-07-14'
	AND RE.cod_pais = entj.cod_pais 
	AND entj.cod_hijo = '10001'
JOIN bi_corp_common.dim_ren_ctaresultadosctr cta ON
	RE.cod_cta_cont_gestion  = cta.cod_ren_cta_resultados
WHERE RE.partition_date = '2020-06-30';

-- RESULT + ENT -- DROP TABLE result_je2
CREATE TEMPORARY TABLE result_je2 AS
SELECT 
	RE.* ,
	ent.cod_ren_entidad_gest_niv_1,
	ent.cod_ren_entidad_gest_niv_2,
	ent.cod_ren_entidad_gest_niv_3,
	ent.cod_ren_entidad_gest_niv_4,
	ent.cod_ren_entidad_gest_niv_5,
	ent.cod_ren_entidad_gest_niv_6,
	ent.ds_ren_entidad_gest_niv_1,
	ent.ds_ren_entidad_gest_niv_2,
	ent.ds_ren_entidad_gest_niv_3,
	ent.ds_ren_entidad_gest_niv_4,
	ent.ds_ren_entidad_gest_niv_5,
	ent.ds_ren_entidad_gest_niv_6,
	offi.cod_ren_ofi_comercial_niv_5 ,
	offi.cod_ren_ofi_comercial_niv_6 ,
	offi.cod_ren_ofi_comercial_niv_7 ,
	offi.cod_ren_ofi_comercial_niv_8 ,
	offi.cod_ren_ofi_comercial_niv_9 ,
	offi.cod_ren_ofi_comercial_niv_10 ,
	offi.cod_ren_ofi_comercial_niv_11 ,
	offi.ds_ren_ofi_comercial_niv_5 ,
	offi.ds_ren_ofi_comercial_niv_6 ,
	offi.ds_ren_ofi_comercial_niv_7 ,
	offi.ds_ren_ofi_comercial_niv_8 ,
	offi.ds_ren_ofi_comercial_niv_9 ,
	offi.ds_ren_ofi_comercial_niv_10 ,
	offi.ds_ren_ofi_comercial_niv_11 ,
	ges.cod_ren_gestor_niv_1 ,
	ges.cod_ren_gestor_niv_2 ,
	ges.cod_ren_gestor_niv_3 ,
	ges.cod_ren_gestor_niv_4 ,
	ges.cod_ren_gestor_niv_5 ,
	ges.cod_ren_gestor_niv_6 ,
	ges.cod_ren_gestor_niv_7 ,
	ges.cod_ren_gestor_niv_8 ,
	ges.cod_ren_gestor_niv_9 ,
	ges.cod_ren_gestor_niv_10 ,
	ges.cod_ren_gestor_niv_11 ,
	ges.ds_ren_gestor_niv_1 ,
	ges.ds_ren_gestor_niv_2 ,
	ges.ds_ren_gestor_niv_3 ,
	ges.ds_ren_gestor_niv_4 ,
	ges.ds_ren_gestor_niv_5 ,
	ges.ds_ren_gestor_niv_6 ,
	ges.ds_ren_gestor_niv_7 ,
	ges.ds_ren_gestor_niv_8 ,
	ges.ds_ren_gestor_niv_9 ,
	ges.ds_ren_gestor_niv_10 ,
	ges.ds_ren_gestor_niv_11
FROM result_je RE
JOIN bi_corp_common.dim_ren_entidadjuridica ent ON
	RE.cod_entidad_espana = ent.cod_ren_entidad_gest 
JOIN bi_corp_common.dim_ren_oficinas offi ON
	RE.cod_ofi_comercial = offi.cod_ren_ofi_comercial
LEFT JOIN bi_corp_common.dim_ren_gestor ges ON 
	RE.cod_gestor_prod = ges.cod_ren_gestor	;

----------------------------------------------------------------
-- DROP TABLE result_g2
CREATE TEMPORARY TABLE result_g AS
SELECT RE.*
	, gen.cod_producto
	, gen.cod_subprodu
	, gen.fec_alta_cto
	, gen.fec_ven
	, gen.fec_reestruc
FROM result_je2 RE
JOIN generic_result gen ON 
	gen.cod_contenido NOT IN ('GAS','RSM','CTB','CCL')
	AND RE.idf_cto_ods = gen.idf_cto_ods
	AND RE.cod_contenido = gen.cod_contenido 
WHERE gen.cod_contenido NOT IN ('GAS','RSM','CTB','CCL')
AND RE.cod_contenido NOT IN ('GAS','RSM','CTB','CCL')
UNION ALL
SELECT RE.*
	, NULL cod_producto
	, NULL cod_subprodu
	, NULL fec_alta_cto
	, NULL fec_ven
	, NULL fec_reestruc
FROM result_je2 RE
WHERE RE.cod_contenido IN ('GAS','RSM','CTB','CCL');

CREATE TEMPORARY TABLE result_gc AS
SELECT RE.*	
	, cli.cod_vincula
	, cli.cod_tip_persona
	, cli.nombre_cliente
FROM result_g RE
JOIN cliente_result cli ON
	cli.idf_pers_ods = RE.idf_pers_ods ;

----------------------------------------------------------------------------------------------
CREATE TABLE bi_corp_staging.bt_ren_blce_result_test 
    row format delimited 
    fields terminated by '|' 
    STORED AS RCFile AS
SELECT * FROM result_gc ;