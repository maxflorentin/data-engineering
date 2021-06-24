"
SET mapred.job.queue.name=root.dataeng;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;

-------------------------------------------------------
-- CREO TABLA TEMPORAL DIVISA DE PARTICION ACTUAL --
-------------------------------------------------------

CREATE TEMPORARY TABLE BI_CORP_COMMON.DIM_COE_DIVISA_TMP AS
SELECT DISTINCT
CASE WHEN COD_BCRA IN ('null','') THEN cast(null as string)
	 ELSE COD_BCRA
END AS COD_BCRA_DIVISA,
CASE WHEN MONCOD IN ('null','') THEN cast(null as string)
	 ELSE MONCOD
END AS COD_DIVISA,
CASE WHEN MONDES IN ('null','') THEN cast(null as string)
	 ELSE MONDES
END AS DESCRIPCION_DIVISA,
CASE WHEN COD_AFIP IN ('null','') THEN cast(null as string)
	 ELSE COD_AFIP
END AS COD_AFIP,
1 AS PARTITION_INSERT
FROM BI_CORP_STAGING.COMEX_RIO39_OPP_MONEDAS
WHERE PARTITION_DATE = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_COMEX-Daily') }}';


--------------------------------------------------------------------
-- COMPARO DIMENSION CON PARTICION ACTUAL Y SELECCIONO LOS UNICOS --
--------------------------------------------------------------------

CREATE TEMPORARY TABLE BI_CORP_COMMON.DIM_COE_DIVISA_TMP_INSERT AS
WITH TABLA_DISTINCT AS (
SELECT * FROM BI_CORP_COMMON.DIM_COE_DIVISA
UNION ALL
SELECT * FROM BI_CORP_COMMON.DIM_COE_DIVISA_TMP
)
SELECT DISTINCT * FROM TABLA_DISTINCT;

------------------------------------------------------------
-- SOBRESCRIBO UNICA PARTICION CON DIMENSION ACTUALIZADA --
------------------------------------------------------------

INSERT OVERWRITE TABLE BI_CORP_COMMON.DIM_COE_DIVISA PARTITION(PARTITION_INSERT)
SELECT * FROM BI_CORP_COMMON.DIM_COE_DIVISA_TMP_INSERT;

DROP TABLE IF EXISTS BI_CORP_COMMON.DIM_COE_DIVISA_TMP;
DROP TABLE IF EXISTS BI_CORP_COMMON.DIM_COE_DIVISA_TMP_INSERT;

"