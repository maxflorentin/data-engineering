set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=4000000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.exec.dynamic.partition.mode=nonstrict;
set mapred.job.queue.name=root.dataeng;

-- Todas las garantias reales 
INSERT OVERWRITE TABLE bi_corp_bdr.JM_GARA_REAL 
PARTITION (partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_Tables-Monthly') }}')
SELECT DISTINCT (concat(substring(gar.partition_date, 1, 8), cast(cal.num_dias_mes AS STRING)) ) AS G4091_FEOPERAC, 
                S1EMP AS G4091_S1EMP, 
				lpad(cast(gar.num_garantia AS string),9,'0') AS G4091_BIENGAR1, 
				id_cto AS G4091_CONTRA1, 
				id_prod_cto AS G4091_ID_PROD, 
				lpad(nvl(COD_BIEN,'0'),5,'0') AS G4091_COD_BIEN, 
				bco_custodio AS G4091_BCO_CUST, 
				IND_INDICE_PRINCIAL AS G4091_BSINDI1, 
				IND_COTIZACION AS G4091_INDCOTIZ, 
				DEUDA_PUBLICA AS G4091_DEUDAPUB, 
				IND_DEUDA AS G4091_INDDEUDA, 
				TIP_FRECUENCIA AS G4091_TIPFRECU, 
				lpad(FRECUENCIA_PLAZO,17,'0') AS G4091_FRECPLAZ,
				AFECTO_EXPLOTACION AS G4091_DKAFECTO, 
				CLA_BIEN AS G4091_CLA_BIEN, 
				lpad(nvl(pais_iso_garante,'0'),5,'0') AS G4091_ID_PAIS, 
				orden_hipoteca AS G4091_ORD_HIPO, 
				cod_divisa AS G4091_CODIVISA, 
				fec_vcto AS G4091_FCHAVCTO, 
				inf_registral AS G4091_INF_REGI, 
				id_emisiones AS G4091_IDEMIS, 
				CODIDPER AS G4091_CODIDPER, 
				IDNUMCLI AS G4091_IDNUMCLI, 
				EMIS_PZO AS G4091_EMIS_PZO, 
				CODMERCD AS G4091_CODMERCD, 
				NUM_TITU AS G4091_NUM_TITU, 
				INDICEA1 AS G4091_INDICEA1, 
				TIP_INTE AS G4091_TIP_INTE, 
				CODCOMPA AS G4091_CODCOMPA, 
				GRADOCOB AS G4091_GRADOCOB, 
				NUM_DOC AS G4091_NUM_DOC, 
				INDACEPT AS G4091_INDACEPT, 
				INDDDOMI AS G4091_INDDDOMI, 
				INDBLO AS G4091_INDBLO, 
				IMCARG AS G4091_IMCARG, 
				(concat(substring(gar.partition_date, 1, 8), cast(cal.num_dias_mes AS STRING)) ) AS G4091_FECULTMO, 
				rpad(nvl(cod_postal,' '),40,' ') AS G4091_CODPOSGR, 
				INAYUDPB AS G4091_INAYUDPB, 
				TIP_HIPO AS G4091_TIP_HIPO
FROM
(
	SELECT DISTINCT -- para no duplicar garantias que aplican a varios contratos
				partition_date,
				'23100' AS S1EMP,		--> Transformar a cod_entidad_bdr
				cast(num_garantia AS string) AS num_garantia,
				'000000000' AS id_cto ,
				'99999' AS id_prod_cto,
				cod_bien,
				lpad('',40,' ') AS bco_custodio,
				'N' AS IND_INDICE_PRINCIAL,
				'N' AS IND_COTIZACION,
				'99999' AS DEUDA_PUBLICA,
				'99999' AS IND_DEUDA, --> "00 No informado" o "99999 No aplica"
				'99999' AS TIP_FRECUENCIA,
				'0' AS FRECUENCIA_PLAZO,
				'N' AS AFECTO_EXPLOTACION,
				lpad('',5,'0') AS CLA_BIEN,
				case when pais_iso_garante = 'AR' then '00032' else pais_iso_garante end AS pais_iso_garante,
				CONCAT(
					LPAD ( CASE WHEN (cod_garantia in ('056', '058', '072', '077', '080') ) THEN '1'
							    WHEN (cod_garantia in ('076', '078') ) THEN '2'	ELSE '0'
				                END , 11,'0'),
					LPAD ('',6,'0')
				) AS orden_hipoteca,
				cod_divisa,
				CASE WHEN (cod_garantia in ('013', '024', '025', '026', '028','036','037','044','047','056','058','071','072','076','078','080') ) THEN fec_vcto
					 ELSE '9999-12-31' END AS fec_vcto,
				lpad('',40,' ') AS inf_registral,
				'99999' AS id_emisiones,
				lpad('',40,' ')	as CODIDPER,
				lpad('',9,'0') AS IDNUMCLI,
				'00000' AS EMIS_PZO,
				'99999' AS CODMERCD,
				lpad('',17,'0') AS NUM_TITU,
				'99999' AS INDICEA1,
				'99999' AS TIP_INTE,
				lpad('',40,' ') AS CODCOMPA,
				lpad('',9,'0') AS GRADOCOB,
				lpad('',40,' ') AS NUM_DOC ,
				'N' AS INDACEPT,
				'N' AS INDDDOMI,
				'S' AS INDBLO,
				lpad('',17,'0') AS IMCARG,
				null AS FECULTMO,
				regexp_replace(COD_POSTAL,'\\|','') AS COD_POSTAL,
				'N' AS INAYUDPB,
				lpad('',5,'0') AS TIP_HIPO
	FROM bi_corp_bdr.aux_garant_garantias_especificas_div
	WHERE partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='last_working_day', dag_id='BDR_LOAD_Tables-Monthly') }}'
	and tip_gara_bdr = '002'  and cod_estado = '020'
	UNION ALL
	SELECT partition_date,
		   '23100' AS S1EMP,
		   cast(num_garantia AS string) AS num_garantia,
		   '000000000' AS id_cto ,
		   '99999' AS id_prod_cto,
		   cod_bien,
		   lpad('',40,' ') AS bco_custodio,
		   'N' AS IND_INDICE_PRINCIAL,
		   'N' AS IND_COTIZACION,
		   '99999' AS DEUDA_PUBLICA,
		   '99999' AS IND_DEUDA, --> "00 No informado" o "99999 No aplica"
		   '99999' AS TIP_FRECUENCIA,
		   '0' AS FRECUENCIA_PLAZO,
		   'N' AS AFECTO_EXPLOTACION,
		   lpad('',5,'0') AS CLA_BIEN,
		   case when pais_iso_garante = 'AR' then '00032' else pais_iso_garante end AS pais_iso_garante,
		   CONCAT(
				LPAD ( CASE WHEN (cod_garantia in ('056', '058', '072', '077', '080') ) THEN '1'
							WHEN (cod_garantia in ('076', '078') ) THEN '2'	ELSE '0'
							END , 11,'0'),
				LPAD ('',6,'0')
		   ) AS orden_hipoteca,
		   cod_divisa,
		   CASE WHEN (cod_garantia in ('013', '024', '025', '026', '028','036','037','044','047','056','058','071','072','076','078','080') ) THEN fec_vcto
			    ELSE '9999-12-31' END AS fec_vcto,
		   lpad('',40,' ') AS inf_registral,
		   '99999' AS id_emisiones,
		   lpad('',40,' ')	as CODIDPER,
		   lpad('',9,'0') AS IDNUMCLI,
		   '00000' AS EMIS_PZO,
		   '99999' AS CODMERCD,
		   lpad('',17,'0') AS NUM_TITU,
		   '99999' AS INDICEA1,
		   '99999' AS TIP_INTE,
		   lpad('',40,' ') AS CODCOMPA,
		   lpad('',9,'0') AS GRADOCOB,
		   lpad('',40,' ') AS NUM_DOC ,
		   'N' AS INDACEPT,
		   'N' AS INDDDOMI,
		   'S' AS INDBLO,
		   lpad('',17,'0') AS IMCARG,
		   null AS FECULTMO,
		   regexp_replace(COD_POSTAL,'\\|','') AS COD_POSTAL,
		   'N' AS INAYUDPB,
		   lpad('',5,'0') AS TIP_HIPO
	FROM bi_corp_bdr.aux_garant_garantias_genericas
	WHERE partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='last_working_day', dag_id='BDR_LOAD_Tables-Monthly') }}'
		AND tip_gara_bdr = '002'  
		AND cod_estado = '020'
) gar
INNER JOIN
	(
		SELECT DISTINCT partition_date, num_garantia
	   	FROM bi_corp_bdr.aux_garant_garantias_contratos_div 
	) garcto
	ON (garcto.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='last_working_day', dag_id='BDR_LOAD_Tables-Monthly') }}'
	AND gar.num_garantia = garcto.num_garantia)
LEFT JOIN santander_business_risk_arda.calendario cal
	ON ( cal.data_date_part = TRANSLATE(gar.partition_date,'-','') 
	AND cal.fec_yyyymmdd = translate(gar.partition_date,'-','') )
WHERE gar.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='last_working_day', dag_id='BDR_LOAD_Tables-Monthly') }}'
;