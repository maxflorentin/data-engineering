
    
   
   
 SELECT * FROM bi_corp_common.bt_ror_input_activo RE
 WHERE RE.partition_date = '2020-12-31' limit 1 ;


ALTER TABLE bi_corp_common.bt_ror_input_activo ADD COLUMNS (cod_ren_idru STRING) cascade ;


describe bi_corp_common.bt_ror_input_activo ;

-- 143.731.779

select count(*) from bi_corp_common.bt_ror_input_activo RE
 WHERE RE.partition_date = '2020-12-31' and cod_ren_areanegociocorp is NULL 
 
 select count(*) from bi_corp_common.bt_ror_input_activo RE
 WHERE RE.partition_date = '2020-12-31'
 
 
 
 
--  
DROP TABLE IF EXISTS  ru ; 
SET hive.exec.dynamic.partition.mode=nonstrict;
SET mapred.job.queue.name=root.dataeng;

CREATE TEMPORARY TABLE ru AS
 WITH rs_activo AS (
	SELECT DISTINCT RE.cod_ren_pers_ods
		, RE.cod_producto_operacional cod_producto
		, RE.cod_ren_subprodu cod_subprodu
		, RE.cod_segmentogest
		, RE.cod_ren_divisa_mis
	FROM bi_corp_common.bt_ror_input_activo RE
	WHERE RE.partition_date = '2020-12-31' 
	AND TRIM(RE.cod_ren_pers_ods) != '-99100'
	AND RE.cod_ren_subprodu IS NOT NULL
	)
    , rs_segmento AS (
	SELECT RE.cod_ren_pers_ods
		, RE.cod_producto
		, RE.cod_subprodu
		, RE.cod_segmentogest
		, RE.cod_ren_divisa_mis
		, TRIM(SE.pesegcal) pesegcal
	FROM rs_activo RE
	LEFT JOIN 
		(SELECT DISTINCT penumper 
			, pesegcal
		FROM bi_corp_staging.malpe_pedt030
		WHERE 	partition_date = '2020-12-30' ) SE
		ON regexp_replace(RE.cod_ren_pers_ods, '10072', '') = SE.penumper
	)
    , rs_baremo AS ( 	 
    SELECT RE.*
    	, BA.cod_baremo_local
        , BG.cod_baremo_global
    FROM rs_segmento RE
    LEFT JOIN bi_corp_bdr.v_baremos_local BA
        ON TRIM(BA.cod_baremo_alfanumerico_local) = TRIM(concat(RE.cod_producto, RE.cod_subprodu))
        AND BA.cod_negocio_local = '3'
    LEFT JOIN bi_corp_bdr.v_map_baremos_global_local BG
        ON BG.cod_baremo_local = BA.cod_baremo_local 
    WHERE BA.cod_baremo_local IS NOT NULL 
    )
    SELECT DISTINCT cod_ren_pers_ods
        , CASE WHEN pesegcal IN ("A", "B", "C", "S") AND cod_baremo_global = "0006" THEN 1100711 
	         WHEN pesegcal IN ("A", "B", "C", "S") AND cod_baremo_global IN ("0013", "0182", "0183") THEN 1100720
	         WHEN pesegcal IN ("A", "B", "C", "S") AND cod_baremo_global = "0017" THEN 1100714
	         WHEN pesegcal IN ("A", "B", "C", "S") AND cod_baremo_global NOT IN ("0017", "0006", "0013", "0182", "0183") THEN 1100717
	         WHEN pesegcal IN ("P1", "C1") THEN 1100709
	         WHEN pesegcal = "P2" THEN 1100540
	         WHEN pesegcal IN ("EM", "G1", "G2") THEN 1100724
	         WHEN pesegcal IN ("IU", "IP") THEN 1100722
	         WHEN cod_baremo_local = 1 AND pesegcal IN ("F2", "OF", "GL", "LA", "LO", "MU", "OT", "FI", "F1") THEN 1040176
	         WHEN cod_baremo_local = 11 AND pesegcal IN ("F2", "OF", "GL", "LA", "LO", "MU", "OT", "FI", "F1") THEN	1040178
	         WHEN cod_baremo_local = 17 AND pesegcal IN ("F2", "OF", "GL", "LA", "LO", "MU", "OT", "FI", "F1") THEN	1040179
	         WHEN pesegcal = "PU" AND cod_ren_divisa_mis = "ARS"  THEN 1200354
	         WHEN pesegcal = "PU" AND cod_ren_divisa_mis <> "ARS"  THEN 1040181 ELSE NULL END id_ru 
FROM rs_baremo ;


DROP TABLE IF EXISTS activo_with_ru ;
SET hive.exec.dynamic.partition.mode=nonstrict;
SET mapred.job.queue.name=root.dataeng;
CREATE TEMPORARY TABLE activo_with_ru AS
SELECT RE.cod_ren_unidad
	, RE.dt_ren_data
	, RE.cod_ren_finalidaddatos
	, RE.cod_ren_contrato_rorac
	, RE.cod_ren_contrato
	, RE.cod_ren_areanegocio
	, RE.cod_ren_divisa
	, RE.cod_ren_division
	, RE.cod_ren_producto
	, RE.cod_ren_pers_ods
	, RE.fc_ren_margenmes
	, RE.fc_ren_sdbsmv
	, RE.fc_ren_sfbsmv
	, RE.dt_ren_fec_altacto
	, RE.dt_ren_fec_ven
	, RE.cod_ren_entidad_espana
	, RE.cod_ren_centrocont
	, RE.cod_ren_subprodu
	, RE.cod_ren_zona
	, RE.cod_ren_territorial
	, RE.cod_ren_gestorprod
	, RE.cod_ren_gestor
	, RE.id_cto_bdr
	, RE.fc_ren_sdbsfm
	, RE.fc_ren_interes
	, RE.fc_ren_comfin
	, RE.fc_ren_comnofin
	, RE.cod_ren_vincula
	, RE.fc_ren_rof
	, RE.fc_ren_tti
	, RE.cod_ren_mtm
	, RE.cod_ren_nocional
	, RE.cod_ren_divisa_mis
	, RE.flag_ren_moralocal
	, RE.flag_ren_carterizadas
	, RE.ds_ren_nombrecliente
	, RE.cod_per_tipopersona
	, RE.cod_ren_nifcif
	, RE.ds_ren_intragrupo
	, RE.cod_ren_internegocio
	, RE.cod_ren_areanegociocorp
	, RE.cod_productogest
	, RE.cod_segmentogest
	, RE.cod_producto_operacional
	, RE.ds_ren_ficheromis
	, RE.dt_ren_fec_reestruc
	, RE.dt_ren_fec_extrdatos
	, RE.flag_ren_neteo
	, RE.fc_ren_orex
	, RE.fc_ifrs_provision
	, RE.fc_ren_costemensualcto
	, RE.cod_nivel_operacion
	, RU.id_ru cod_ren_idru
	, RE.partition_date
FROM bi_corp_common.bt_ror_input_activo RE
LEFT JOIN ru RU 
	ON RU.cod_ren_pers_ods = RE.cod_ren_pers_ods
WHERE RE.partition_date = '2020-12-31' ;




SELECT COUNT(1) FROM (
SELECT DISTINCT * FROM activo_with_ru
)A ; -- 143.731.779 -- 143.731.830


SET hive.exec.dynamic.partition.mode=nonstrict;
SET mapred.job.queue.name=root.dataeng;
INSERT OVERWRITE TABLE bi_corp_common.bt_ror_input_activo
PARTITION(partition_date)
SELECT DISTINCT * FROM activo_with_ru ;



-- C  
SET mapred.job.queue.name=root.dataeng;
WITH rs_activo AS (
	SELECT DISTINCT RE.cod_ren_pers_ods
		, RE.cod_producto_operacional cod_producto
		, RE.cod_ren_subprodu cod_subprodu
		, RE.cod_segmentogest
		, RE.cod_ren_divisa_mis
	FROM bi_corp_common.bt_ror_input_activo RE
	WHERE RE.partition_date = '2020-12-31' 
	)
    , rs_segmento AS (
	SELECT RE.cod_ren_pers_ods
		, RE.cod_producto
		, RE.cod_subprodu
		, RE.cod_segmentogest
		, RE.cod_ren_divisa_mis
		, TRIM(SE.pesegcal) pesegcal
	FROM rs_activo RE
	LEFT JOIN 
		(SELECT DISTINCT penumper 
			, pesegcal
		FROM bi_corp_staging.malpe_pedt030
		WHERE 	partition_date = '2020-12-30' ) SE
		ON regexp_replace(RE.cod_ren_pers_ods, '10072', '') = TRIM(SE.penumper)
	)
    , rs_baremo AS ( 	 
    SELECT RE.*
    	, BA.cod_baremo_local
        , BG.cod_baremo_global
    FROM rs_segmento RE
    LEFT JOIN bi_corp_bdr.v_baremos_local BA
        ON TRIM(BA.cod_baremo_alfanumerico_local) = TRIM(concat(RE.cod_producto, RE.cod_subprodu))
        AND BA.cod_negocio_local = '3'
    LEFT JOIN bi_corp_bdr.v_map_baremos_global_local BG
        ON BG.cod_baremo_local = BA.cod_baremo_local 
    WHERE BA.cod_baremo_local IS NOT NULL 
    )
    SELECT DISTINCT cod_ren_pers_ods
        , CASE WHEN pesegcal IN ("A", "B", "C", "S") AND cod_baremo_global = "0006" THEN 1200009 
	         WHEN pesegcal IN ("A", "B", "C", "S") AND cod_baremo_global IN ("0013", "0182", "0183") THEN 1200041
	         WHEN pesegcal IN ("A", "B", "C", "S") AND cod_baremo_global = "0017" THEN 1200073
	         WHEN pesegcal IN ("A", "B", "C", "S") AND cod_baremo_global NOT IN ("0017", "0006", "0013", "0182", "0183") THEN 1200105
	         WHEN pesegcal IN ("P1", "C1") THEN 1200137
	         WHEN pesegcal = "P2" THEN 1200169
	         WHEN pesegcal IN ("EM", "G1", "G2") THEN 1200201
	         WHEN pesegcal IN ("IU", "IP") THEN 1200233
	         WHEN cod_baremo_local = 1 AND pesegcal IN ("F2", "OF", "GL", "LA", "LO", "MU", "OT", "FI", "F1") THEN 1040176
	         WHEN cod_baremo_local = 11 AND pesegcal IN ("F2", "OF", "GL", "LA", "LO", "MU", "OT", "FI", "F1") THEN	1200290
	         WHEN cod_baremo_local = 17 AND pesegcal IN ("F2", "OF", "GL", "LA", "LO", "MU", "OT", "FI", "F1") THEN	1200322
	         WHEN pesegcal = "PU" AND cod_ren_divisa_mis = "ARS"  THEN 1200354
	         WHEN pesegcal = "PU" AND cod_ren_divisa_mis <> "ARS"  THEN 1040181 ELSE NULL END id_ru 
	    , pesegcal 
	    , cod_baremo_local
	    , cod_baremo_global
	    , cod_producto
		, cod_subprodu
		, cod_segmentogest
		, cod_ren_divisa_mis
FROM rs_baremo ;

SELECT * FROM activo_with_ru where cod_ren_pers_ods = '1007200031558';


-- 1007200031558 00031558

SELECT DISTINCT penumper
	, pesegcal
FROM bi_corp_staging.malpe_pedt030
WHERE 	partition_date = '2020-12-30'
AND penumper = '00031558'
   
   