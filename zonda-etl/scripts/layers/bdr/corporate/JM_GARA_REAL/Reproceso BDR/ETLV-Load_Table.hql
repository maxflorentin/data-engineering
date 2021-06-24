set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=4000000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.exec.dynamic.partition.mode=nonstrict;
set mapred.job.queue.name=root.dataeng;


-- Todas las garantias reales 

INSERT OVERWRITE TABLE bi_corp_bdr.JM_GARA_REAL PARTITION (partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201912_202103') }}')
SELECT DISTINCT
	  (concat(substring(gar.partition_date, 1, 8), cast(cal.num_dias_mes as STRING)) ) as G4091_FEOPERAC
	, S1EMP as G4091_S1EMP
	, lpad(cast(gar.num_garantia as string),9,'0') as G4091_BIENGAR1
	, id_cto as G4091_CONTRA1
	, id_prod_cto as G4091_ID_PROD
	, lpad(nvl(COD_BIEN,'0'),5,'0') as G4091_COD_BIEN
	, bco_custodio as G4091_BCO_CUST
	, IND_INDICE_PRINCIAL as G4091_BSINDI1
	, IND_COTIZACION as G4091_INDCOTIZ
	, DEUDA_PUBLICA as G4091_DEUDAPUB
	, IND_DEUDA as G4091_INDDEUDA
	, TIP_FRECUENCIA as G4091_TIPFRECU
	, lpad(FRECUENCIA_PLAZO,17,'0') as G4091_FRECPLAZ
	, AFECTO_EXPLOTACION as G4091_DKAFECTO
	, CLA_BIEN as G4091_CLA_BIEN
	, lpad(nvl(pais_iso_garante,'0'),5,'0') as G4091_ID_PAIS
	, orden_hipoteca as G4091_ORD_HIPO
	, cod_divisa as G4091_CODIVISA
	, fec_vcto as G4091_FCHAVCTO
	, inf_registral as G4091_INF_REGI
	, id_emisiones as G4091_IDEMIS
	, CODIDPER as G4091_CODIDPER
	, IDNUMCLI as G4091_IDNUMCLI
	, EMIS_PZO as G4091_EMIS_PZO
	, CODMERCD as G4091_CODMERCD
	, NUM_TITU as G4091_NUM_TITU
	, INDICEA1 as G4091_INDICEA1
	, TIP_INTE as G4091_TIP_INTE
	, CODCOMPA as G4091_CODCOMPA
	, GRADOCOB as G4091_GRADOCOB
	, NUM_DOC as G4091_NUM_DOC
	, INDACEPT as G4091_INDACEPT
	, INDDDOMI as G4091_INDDDOMI
	, INDBLO as G4091_INDBLO
	, IMCARG as G4091_IMCARG
	, (concat(substring(gar.partition_date, 1, 8), cast(cal.num_dias_mes as STRING)) ) as G4091_FECULTMO
	, rpad(nvl(cod_postal,' '),40,' ') as G4091_CODPOSGR
	, INAYUDPB as G4091_INAYUDPB
	, TIP_HIPO as G4091_TIP_HIPO
FROM
(
	SELECT DISTINCT -- para no duplicar garantias que aplican a varios contratos
		partition_date,
		'23100' as S1EMP,		--> Transformar a cod_entidad_bdr
		cast(num_garantia as string) as num_garantia,
		'000000000' as id_cto ,
		'99999' as id_prod_cto,
		cod_bien,
		lpad('',40,' ') as bco_custodio,
		'N' as IND_INDICE_PRINCIAL,
		'N' as IND_COTIZACION,
		'99999' as DEUDA_PUBLICA,
		'99999' as IND_DEUDA, --> "00 No informado" o "99999 No aplica"
		'99999' as TIP_FRECUENCIA,
		'0' as FRECUENCIA_PLAZO,
		'N' as AFECTO_EXPLOTACION,
		lpad('',5,'0') as CLA_BIEN,
		case when pais_iso_garante = 'AR' then '00032' else pais_iso_garante end as pais_iso_garante,
		CONCAT(
			LPAD ( CASE WHEN (cod_garantia in ('056', '058', '072', '077', '080') ) THEN '1'
					WHEN (cod_garantia in ('076', '078') ) THEN '2'	ELSE '0'
				END , 11,'0'),
			LPAD ('',6,'0')
			)
		as orden_hipoteca,
		cod_divisa,
		CASE
			WHEN (cod_garantia in ('013', '024', '025', '026', '028','036','037','044','047','056','058','071','072','076','078','080') ) THEN fec_vcto
			ELSE '9999-12-31' END
		as fec_vcto,
		lpad('',40,' ') as inf_registral,
		'99999' as id_emisiones,
		lpad('',40,' ')	as CODIDPER,
		lpad('',9,'0') as IDNUMCLI,
		'00000' as EMIS_PZO,
		'99999' as CODMERCD,
		lpad('',17,'0') as NUM_TITU,
		'99999' as INDICEA1,
		'99999' as TIP_INTE,
		lpad('',40,' ') as CODCOMPA,
		lpad('',9,'0') as GRADOCOB,
		lpad('',40,' ') as NUM_DOC ,
		'N' as INDACEPT,
		'N' as INDDDOMI,
		'S' as INDBLO,
		lpad('',17,'0') as IMCARG,
		null as FECULTMO,
		COD_POSTAL,
		'N' as INAYUDPB,
		lpad('',5,'0') as TIP_HIPO
	from bi_corp_bdr.aux_garant_garantias_especificas_div
	where partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='last_working_day', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201912_202103') }}'
	and tip_gara_bdr = '002'  and cod_estado = '020'
UNION ALL
	SELECT
		partition_date,
		'23100' as S1EMP,
		cast(num_garantia as string) as num_garantia,
		'000000000' as id_cto ,
		'99999' as id_prod_cto,
		cod_bien,
		lpad('',40,' ') as bco_custodio,
		'N' as IND_INDICE_PRINCIAL,
		'N' as IND_COTIZACION,
		'99999' as DEUDA_PUBLICA,
		'99999' as IND_DEUDA, --> "00 No informado" o "99999 No aplica"
		'99999' as TIP_FRECUENCIA,
		'0' as FRECUENCIA_PLAZO,
		'N' as AFECTO_EXPLOTACION,
		lpad('',5,'0') as CLA_BIEN,
		case when pais_iso_garante = 'AR' then '00032' else pais_iso_garante end as pais_iso_garante,
		CONCAT(
			LPAD ( CASE WHEN (cod_garantia in ('056', '058', '072', '077', '080') ) THEN '1'
					WHEN (cod_garantia in ('076', '078') ) THEN '2'	ELSE '0'
				END , 11,'0'),
			LPAD ('',6,'0')
			)
		as orden_hipoteca,
		cod_divisa,
		CASE
			WHEN (cod_garantia in ('013', '024', '025', '026', '028','036','037','044','047','056','058','071','072','076','078','080') ) THEN fec_vcto
			ELSE '9999-12-31' END
		as fec_vcto,
		lpad('',40,' ') as inf_registral,
		'99999' as id_emisiones,
		lpad('',40,' ')	as CODIDPER,
		lpad('',9,'0') as IDNUMCLI,
		'00000' as EMIS_PZO,
		'99999' as CODMERCD,
		lpad('',17,'0') as NUM_TITU,
		'99999' as INDICEA1,
		'99999' as TIP_INTE,
		lpad('',40,' ') as CODCOMPA,
		lpad('',9,'0') as GRADOCOB,
		lpad('',40,' ') as NUM_DOC ,
		'N' as INDACEPT,
		'N' as INDDDOMI,
		'S' as INDBLO,
		lpad('',17,'0') as IMCARG,
		null as FECULTMO,
		COD_POSTAL,
		'N' as INAYUDPB,
		lpad('',5,'0') as TIP_HIPO
	from bi_corp_bdr.aux_garant_garantias_genericas
	where partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='last_working_day', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201912_202103') }}'
	and tip_gara_bdr = '002'  and cod_estado = '020'
) gar
INNER JOIN
	(SELECT distinct partition_date, num_garantia
	   FROM bi_corp_bdr.aux_garant_garantias_contratos_div ) garcto
	ON (garcto.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='last_working_day', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201912_202103') }}'
	and gar.num_garantia = garcto.num_garantia)
LEFT JOIN santander_business_risk_arda.calendario cal
	ON ( cal.data_date_part = TRANSLATE(gar.partition_date,'-','') AND cal.fec_yyyymmdd = translate(gar.partition_date,'-','') )
where gar.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='last_working_day', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201912_202103') }}'
;