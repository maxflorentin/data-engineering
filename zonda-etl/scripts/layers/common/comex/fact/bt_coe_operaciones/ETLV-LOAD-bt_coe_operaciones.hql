"
SET mapred.job.queue.name=root.dataeng;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;

---------------------------------------------------------------------
-- CREO TABLA TEMPORAL INTERFACES_COMPLETA - BUSCA NUP DEL TITULAR --
---------------------------------------------------------------------

DROP TABLE IF EXISTS BI_CORP_COMMON.COMEX_INTERFACES_COMPLETA;

CREATE TEMPORARY TABLE BI_CORP_COMMON.COMEX_INTERFACES_COMPLETA AS
WITH TABLA_DISTINCT AS (
SELECT * FROM BI_CORP_STAGING.COMEX_INTERFACES_TRF
UNION ALL
SELECT * FROM BI_CORP_STAGING.COMEX_INTERFACES_P
UNION ALL
SELECT * FROM BI_CORP_STAGING.COMEX_INTERFACES_BKT
),
TABLA_OPERACIONES AS (
SELECT DISTINCT * FROM TABLA_DISTINCT
),
SUC_CTA AS (
SELECT DISTINCT LPAD(NVL(CTA_SUC,'0000'),4,'0') AS P_CTA_SUC, NVL(CTA_TIPO,'0') AS P_CTA_TIPO, LPAD(NVL(CTA_OPERAC,'000000000000'),12,'0') AS P_CTA_OPERAC, NRO_OPERAC AS NRO_OPERACION FROM TABLA_OPERACIONES
),
TIPO_CUENTA AS(
SELECT *,
CASE WHEN P_CTA_TIPO = '0' THEN '05'
	 WHEN P_CTA_TIPO = '1' THEN '02'
	 WHEN P_CTA_TIPO = '3' THEN '06'
	 WHEN P_CTA_TIPO = '4' THEN '03'
	 WHEN P_CTA_TIPO = '5' THEN '05'
	 WHEN P_CTA_TIPO = '6' THEN '06'
	 WHEN P_CTA_TIPO = '7' THEN '05'
	 ELSE '07'
END AS TIPO_CUENTA_DECODE
FROM SUC_CTA
),
NUP_T AS (
SELECT DISTINCT B.NRO_OPERACION, LPAD(NVL(A.PENUMPER,'00000000'),8,'0') AS NUP_TITULAR
FROM TIPO_CUENTA B
LEFT JOIN BI_CORP_STAGING.MALPE_PEDT008 A ON A.PECODOFI = B.P_CTA_SUC AND A.PECODPRO = B.TIPO_CUENTA_DECODE AND A.PENUMCON = B.P_CTA_OPERAC AND A.PEESTREL = 'A' AND A.PEFECBRB = '9999-12-31' AND A.PECODPRO IN ('02','03','04','05','06','07','98','99') AND A.PECALPAR = 'TI' AND A.PARTITION_DATE = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Comex-Daily') }}'
)
SELECT A.*, B.NUP_TITULAR FROM TABLA_OPERACIONES A
LEFT JOIN NUP_T B ON B.NRO_OPERACION = A.NRO_OPERAC;


------------------------------------------------------------------------------------
-- CREO TABLA TEMPORAL INTERFACES_COMPLETA - BUSCA CTA OPERACION Y PAIS ORDENANTE --
------------------------------------------------------------------------------------

DROP TABLE IF EXISTS BI_CORP_COMMON.COMEX_INTERFACES_COMPLETA_V2;

CREATE TEMPORARY TABLE BI_CORP_COMMON.COMEX_INTERFACES_COMPLETA_V2 AS
WITH CTA_OPERAC AS (
SELECT DISTINCT CONCAT(LPAD(VM.PECODPRO,3,'0'),LPAD(TRIM(NVL(I.CTA_OPERAC,'')),9,'0')) AS L_CTA_OPERAC, I.NRO_OPERAC
FROM BI_CORP_STAGING.MALPE_PEDT008 VM
JOIN BI_CORP_COMMON.COMEX_INTERFACES_COMPLETA I ON VM.PECODOFI = LPAD(TRIM(I.CTA_SUC),4,'0') AND VM.PENUMCON =  LPAD(TRIM(I.CTA_OPERAC ),12,'0') AND VM.PENUMPER = LPAD(TRIM(I.NUP),8,'0')
WHERE VM.PECDGENT = '0072'
AND VM.PEESTREL = 'A'
AND VM.PECALPAR = 'TI'
AND VM.PEFECBRB = '9999-12-31'
AND VM.PECODPRO IN ('02','03','04','05', '06','07', '98','99')
AND VM.PARTITION_DATE = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Comex-Daily') }}'
),
PAIS_ORDENANTE AS (
SELECT DISTINCT SUBSTR (P.DESCRIPCION, 1, 35) L_PAIS_ORDENANTE, I.NRO_OPERAC
FROM BI_CORP_STAGING.COMEX_RIO39_PAISES P
JOIN BI_CORP_COMMON.COMEX_INTERFACES_COMPLETA I ON P.COD_BCRA = TRIM(I.BENEFI_COD_PAIS)
WHERE P.PARTITION_DATE = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Comex-Daily') }}'
)
SELECT A.*,
B.L_PAIS_ORDENANTE,
CASE WHEN C.L_CTA_OPERAC IS NOT NULL THEN C.L_CTA_OPERAC
	 ELSE LPAD(TRIM(A.CTA_OPERAC),12,'0')
END AS L_CTA_OPERAC
FROM BI_CORP_COMMON.COMEX_INTERFACES_COMPLETA A
LEFT JOIN PAIS_ORDENANTE B ON A.NRO_OPERAC = B.NRO_OPERAC
LEFT JOIN CTA_OPERAC C ON A.NRO_OPERAC = C.NRO_OPERAC;



---------------------------------------------------------------------------------------
-- CREO TABLA FISICA INTERFACES_COMPLETA EN STAGING CON FILTROS PARA INSERTAR EN SKT --
---------------------------------------------------------------------------------------

DROP TABLE IF EXISTS BI_CORP_STAGING.COMEX_INTERFACES_COMPLETA_FILTRADA;

CREATE TABLE BI_CORP_STAGING.COMEX_INTERFACES_COMPLETA_FILTRADA AS
SELECT DISTINCT
I.COD_MONEDA,
M.COD_BCRA DIVISA,
'D' FRECUENCIA,
50 PROD_CTBLE,
'O' TIPOINFO,
I.SUBPRODUCTO,
CASE WHEN I.FECHA_OPERACION IS NOT NULL OR I.FECHA_OPERACION <> 'null' THEN I.FECHA_OPERACION
	 ELSE I.FECHA_PROCESO
END AS FECHA_ALTA,
NULL AS FECHA_BAJA,
CASE WHEN I.FECHA_VENCIMIENTO = '' THEN NULL
	 ELSE I.FECHA_VENCIMIENTO
END AS FECHA_VTO,
I.FECHA_CARGA,
'V' ESTADO,
CASE WHEN TRIM(I.CTA_SUC) IN ('','null') THEN NULL
	 ELSE LPAD(TRIM(I.CTA_SUC), 5, '0')
END AS CTA_SUC,
I.CTA_OPERAC CTA_OPERAC,
CAST(I.IMP_TIPO_CAMBIO_OPERAC AS DECIMAL(38,5)) IMP_TIPO_CAMBIO_OPERAC,
'00000' SUC_TRAN,
'' CANAL_TRAN,
I.NRO_OPERAC,
CAST(I.IMP_ALTA_OPERAC_ME AS DECIMAL(38,5)) IMP_ALTA_OPERAC_ME,
LPAD (NVL (TRIM (I.NUP), '0'), 8, '0') NUP,
'01' ENTIDAD_JURID,
SUBSTR (I.BENEFI_EXTERIOR, 1, 20) BENEFI_EXTERIOR,
I.BENEFI_DOM,
I.ORIGEN,
SUBSTR (I.BCO_CORR_RIO, 1, 30) BCO_CORR_RIO,
I.BANCO_CORRE_COD,
SUBSTR (I.BANCO_CORRE, 1, 20) BANCO_CORRE,
SUBSTR (I.BANCO_CORRE_DOM, 1, 20) BANCO_CORRE_DOM,
SUBSTR (I.BANCO_CORRE_PAIS, 1, 20) BANCO_CORRE_PAIS,
SUBSTR (I.BANCO_CORRE_CIUDAD, 1, 20) CIUDAD,
I.BENEFI_COD_PAIS,
I.FECHA_PROCESO,
I.FECHA_PROC FECHA_PROC,
I.CTA_TIPO CTA_TIPO,
CAST(I.IMP_OPERAC_PESOS AS DECIMAL(38,5)) IMP_OPERAC_PESOS,
CAST(I.IMP_TIPO_CAMBIO_DIA AS DECIMAL(38,5)) IMP_TIPO_CAMBIO_DIA,
I.CONCEPTOBCRA,
I.CUENTA_EN,
I.NUP_TITULAR,
L_PAIS_ORDENANTE,
L_CTA_OPERAC,
I.SECTOR
FROM BI_CORP_COMMON.COMEX_INTERFACES_COMPLETA_V2 I
JOIN BI_CORP_STAGING.COMEX_RIO39_INTERFACES_TIPO_MOV T ON T.TIPO_MOV = I.TIPO_MOV AND T.INTERFACE IN ('TODAS', 'ESTA') AND T.PARTITION_DATE = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Comex-Daily') }}'
JOIN BI_CORP_STAGING.COMEX_RIO39_OPP_MONEDAS M ON I.COD_MONEDA = M.MONCOD AND M.PARTITION_DATE = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Comex-Daily') }}'
WHERE I.SUBPRODUCTO NOT IN ('EXCL')
AND ((I.TIPO_MOV <> '070') OR ( I.TIPO_MOV = '070' AND SUBSTR (I.NRO_OPERAC, 1, 1) IN ('L', 'M', 'N', 'Z') ));

---------------------------------------------------------------------------------
-- SELECCIONO CAMPOS Y ORGANIZO - INSERTAR EN SKT - BORRA TMP - FIN DE PROCESO --
---------------------------------------------------------------------------------

DROP TABLE IF EXISTS BI_CORP_COMMON.BT_COE_OPERACIONES_D_TMP;

CREATE TEMPORARY TABLE BI_CORP_COMMON.BT_COE_OPERACIONES_D_TMP AS
WITH NOMBRE_CAMPOS AS (
SELECT
I.FRECUENCIA				AS FRECUENCIA,
I.PROD_CTBLE				AS PROD_CTBLE,
I.SUBPRODUCTO				AS SUBPRODUCTO,
I.TIPOINFO					AS TIPOINFO,
I.FECHA_ALTA				AS FECHA_ALTA,
I.FECHA_BAJA                AS FECHA_BAJA,
I.FECHA_VTO                 AS FECHA_VTO,
I.FECHA_CARGA				AS FECHA_CARGA,
I.ESTADO					AS ESTADO,
I.CTA_SUC					AS SUC_OPERAC,
I.L_CTA_OPERAC				AS CTA_OPERAC,
I.DIVISA					AS DIVISA,
I.IMP_TIPO_CAMBIO_DIA		AS TIPO_CAMBIO,
I.IMP_TIPO_CAMBIO_OPERAC	AS TIPO_CAMBIO_OPERAC,
I.SUC_TRAN					AS SUC_TRAN,
I.CANAL_TRAN				AS CANAL_TRAN,
I.NRO_OPERAC				AS NRO_OPERAC,
I.IMP_ALTA_OPERAC_ME		AS IMP_OPERAC,
I.IMP_ALTA_OPERAC_ME		AS IMP_ALTA_OPERAC,
I.NUP						AS NUP,
I.ENTIDAD_JURID				AS ENTIDAD_JURID,
I.BANCO_CORRE_PAIS			AS PAIS,
I.BANCO_CORRE				AS BANCO_CORRE,
I.BENEFI_EXTERIOR			AS ORDENANTE,
I.FECHA_PROC                AS FECHA_PROC,
I.CIUDAD					AS CIUDAD,
I.ORIGEN					AS ORIGEN,
I.BANCO_CORRE_DOM			AS DOM_BANCO_CORRE,
I.L_PAIS_ORDENANTE			AS PAIS_ORDENANTE,
I.BENEFI_DOM				AS DOM_ORDENANTE,
I.BCO_CORR_RIO				AS BCO_CORR_RIO,
CASE WHEN I.IMP_OPERAC_PESOS IS NULL AND I.IMP_TIPO_CAMBIO_OPERAC NOT IN (0,-1) THEN (I.IMP_ALTA_OPERAC_ME * I.IMP_TIPO_CAMBIO_OPERAC)
     ELSE
         CASE WHEN I.IMP_OPERAC_PESOS IS NULL AND I.IMP_TIPO_CAMBIO_OPERAC IN (0,-1) THEN (I.IMP_ALTA_OPERAC_ME * I.IMP_TIPO_CAMBIO_DIA)
              ELSE I.IMP_OPERAC_PESOS
         END
END AS IMP_OPERAC_ARS,
I.NUP_TITULAR				AS NUP_TITULAR,
I.CONCEPTOBCRA				AS CONCEPTOBCRA,
CASE WHEN I.CUENTA_EN IS NULL OR I.CUENTA_EN IN ('null','') THEN '080'
	 ELSE I.CUENTA_EN
END 						AS CUENTA_EN,
I.SECTOR					AS SECTOR,
I.FECHA_PROCESO				AS FECHA_PROCESO
FROM BI_CORP_STAGING.COMEX_INTERFACES_COMPLETA_FILTRADA I
)
SELECT DISTINCT
TRIM(FRECUENCIA) 			AS COD_COE_FRECUENCIA,
PROD_CTBLE 		   			AS COD_COE_PRODUCTO_CONTAB,
TRIM(SUBPRODUCTO) 	   		AS COD_COE_SUBPRODUCTO,
TRIM(TIPOINFO) 	       		AS COD_COE_TIPO_INFORMACION,
FECHA_ALTA 		   			AS DT_COE_FECHA_ALTA,
FECHA_BAJA 		   			AS DT_COE_FECHA_BAJA,
FECHA_VTO  		   			AS DT_COE_FECHA_VENC,
TRIM(ESTADO) 				AS COD_COE_ESTADO,
CAST(SUC_OPERAC AS INT) 	AS COD_COE_SUCURSAL,
CAST(CTA_OPERAC AS INT) 	AS COD_COE_CUENTA,
TRIM(DIVISA)     			AS COD_COE_MONEDA,
TIPO_CAMBIO		   			AS FC_COE_TIPO_CAMBIO_USD,
TIPO_CAMBIO_OPERAC 			AS FC_COE_TIPO_CAMBIO_OPER,
TRIM(SUC_TRAN)		   		AS COD_COE_SUC_TRX,
TRIM(ORIGEN)            	AS COD_COE_CANAL_TRX,
TRIM(NRO_OPERAC)			AS COD_COE_NRO_OPERACION,
IMP_OPERAC         			AS FC_COE_IMPORTE,
IMP_ALTA_OPERAC    			AS FC_COE_IMPORTE_ALTA,
CAST(NUP AS INT)        	AS COD_COE_NUP,
TRIM(ENTIDAD_JURID)	   		AS COD_COE_ENTIDAD_JURIDICA,
TRIM(PAIS)              	AS DS_COE_PAIS,
FECHA_PROCESO	   			AS DT_COE_FECHA_PROCESO,
TRIM(BANCO_CORRE)       	AS DS_COE_BANCO_CORRESPONSAL,
TRIM(ORDENANTE)         	AS DS_COE_CONTRAPARTE,
TRIM(ORIGEN) 				AS COD_COE_CANAL_VENTA,
FECHA_CARGA 	   			AS DT_COE_FECHA_CARGA,
TRIM(CIUDAD) 				AS DS_COE_CIUDAD_BANCO_CORRESP,
TRIM(SECTOR) 		  		AS DS_COE_SECTOR,
TRIM(DOM_ORDENANTE)	   		AS DS_COE_DOMICILIO_CONTRAPARTE,
TRIM(PAIS_ORDENANTE) 		AS DS_COE_PAIS_ORDENANTE,
TRIM(BCO_CORR_RIO) 	   		AS DS_COE_BANCO_CORRESPONSAL_RIO,
IMP_OPERAC_ARS 	   			AS FC_COE_IMPORTE_MONEDA_NACIONAL,
CAST(NUP_TITULAR AS INT)	AS COD_COE_NUP_TITULAR,
TRIM(CONCEPTOBCRA) 	   		AS COD_COE_CONCEPTOBCRA,
TRIM(CUENTA_EN) 			AS COD_COE_MONEDA_CUENTA_ORIG,
FECHA_PROCESO	   			AS PARTITION_DATE
FROM NOMBRE_CAMPOS;

INSERT OVERWRITE TABLE BI_CORP_COMMON.BT_COE_OPERACIONES PARTITION(PARTITION_DATE)
SELECT * FROM BI_CORP_COMMON.BT_COE_OPERACIONES_D_TMP;


DROP TABLE IF EXISTS BI_CORP_COMMON.COMEX_INTERFACES_COMPLETA;
DROP TABLE IF EXISTS BI_CORP_COMMON.COMEX_INTERFACES_COMPLETA_V2;
DROP TABLE IF EXISTS BI_CORP_COMMON.BT_COE_OPERACIONES_D_TMP;
DROP TABLE IF EXISTS BI_CORP_STAGING.COMEX_INTERFACES_TRF;
DROP TABLE IF EXISTS BI_CORP_STAGING.COMEX_INTERFACES_P;
DROP TABLE IF EXISTS BI_CORP_STAGING.COMEX_INTERFACES_BKT;
DROP TABLE IF EXISTS BI_CORP_STAGING.COMEX_INTERFACES_COMPLETA_FILTRADA;

"