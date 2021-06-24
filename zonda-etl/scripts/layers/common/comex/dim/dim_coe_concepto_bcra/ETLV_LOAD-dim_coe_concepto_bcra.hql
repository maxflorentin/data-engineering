"
SET mapred.job.queue.name=root.dataeng;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;

-------------------------------------------------------
-- CREO TABLA TEMPORAL CONCEPTOS DE PARTICION ACTUAL --
-------------------------------------------------------

CREATE TEMPORARY TABLE BI_CORP_COMMON.DIM_COE_CONCEPTO_BCRA_TMP AS
SELECT DISTINCT
CODIGO AS COD_BCRA,
CASE WHEN ALIAS1 IN ('null','') THEN cast(null as string)
	 ELSE ALIAS1
END AS ALIAS,
CASE WHEN DESCRIPCION IN ('null','') THEN cast(null as string)
	 ELSE DESCRIPCION
END AS DESCRIPCION,
CASE WHEN EXTENDIDO IN ('null','') THEN cast(null as string)
	 ELSE EXTENDIDO
END AS EXTENDIDO,
CASE WHEN COD_AFIP IN ('null','') THEN cast(null as string)
	 ELSE COD_AFIP
END AS COD_AFIP,
1 AS PARTITION_INSERT
FROM BI_CORP_STAGING.RIO4_CONCEPTOSBCRA
WHERE PARTITION_DATE = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_COMEX-Daily') }}'


--------------------------------------------------------------------
-- COMPARO DIMENSION CON PARTICION ACTUAL Y SELECCIONO LOS UNICOS --
--------------------------------------------------------------------

CREATE TEMPORARY TABLE BI_CORP_COMMON.DIM_COE_CONCEPTO_BCRA_TMP_INSERT AS
WITH TABLA_DISTINCT AS (
SELECT * FROM BI_CORP_COMMON.DIM_COE_CONCEPTO_BCRA
UNION ALL
SELECT * FROM BI_CORP_COMMON.DIM_COE_CONCEPTO_BCRA_TMP
)
SELECT DISTINCT * FROM TABLA_DISTINCT;

------------------------------------------------------------
-- SOBRESCRIBO UNICA PARTICION CON DIMENSION ACTUALIZADA --
------------------------------------------------------------

INSERT OVERWRITE TABLE BI_CORP_COMMON.DIM_COE_CONCEPTO_BCRA PARTITION(PARTITION_INSERT)
SELECT * FROM BI_CORP_COMMON.DIM_COE_CONCEPTO_BCRA_TMP_INSERT

DROP TABLE IF EXISTS BI_CORP_COMMON.DIM_COE_CONCEPTO_BCRA_TMP;
DROP TABLE IF EXISTS BI_CORP_COMMON.DIM_COE_CONCEPTO_BCRA_TMP_INSERT;

"