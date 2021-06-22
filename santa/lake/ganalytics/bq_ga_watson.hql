#StandardSQL
with tabla as (
SELECT
clientid,
fullvisitorid,
       CAST(FORMAT_TIMESTAMP("%d/%m/%Y %H:%M:%S",
            TIMESTAMP_SECONDS(CAST(visitStartTime + ei.time/1000 AS INT64)),
            "America/Argentina/Buenos_Aires") AS STRING) AS TIMESTAMP,
       ei.eventInfo.eventAction AS TIPO_INTERACCION,
       REPLACE(REPLACE(ei.eventInfo.eventLabel,",","."), ";", "") AS TEXTO_INTERACCION,
       (SELECT P.value FROM UNNEST(ei.customDimensions) AS P WHERE P.index = 9) AS TIPO_RESPUESTA,
       (SELECT P.value FROM UNNEST(ei.customDimensions) AS P WHERE P.index = 10) AS INTENCION,
       (SELECT P.value FROM UNNEST(ei.customDimensions) AS P WHERE P.index = 11) AS CONVERSACION_ID,
	     CASE WHEN REGEXP_CONTAINS(ei.page.hostname, "www2.personas") = true OR REGEXP_CONTAINS(ei.page.hostname, "www.personas") = true THEN "OBP"
            WHEN ei.page.hostname IS NULL THEN
				CASE WHEN (ei.appInfo.appVersion LIKE "3%" AND (SUBSTR(ei.appInfo.appVersion, 3, 1) >= "5")) THEN "APP ANDROID"
					 WHEN (ei.appInfo.appVersion LIKE "2%" AND (SUBSTR(ei.appInfo.appVersion, 3, 1) >= "4")) THEN "APP IOS"
			END
            ELSE IFNULL((SELECT P.value FROM UNNEST(ei.customDimensions) AS P WHERE P.index = 18),"-") END
            AS CHANNEL,
       (SELECT P.value FROM UNNEST(ei.customMetrics) AS P WHERE P.index = 1) AS CONFIANZA,
       --IFNULL((SELECT P.value FROM UNNEST(ei.customDimensions) AS P WHERE P.index = 59),ei.page.pagePath) AS PANTALLA_APERTURA,
       CASE WHEN ei.page.hostname IS NULL THEN ei.appInfo.screenName
       ELSE IFNULL((SELECT P.value FROM UNNEST(ei.customDimensions) AS P WHERE P.index = 59),ei.page.pagePath)
       END AS PANTALLA_APERTURA,
       (SELECT P.value FROM UNNEST(ei.customDimensions) AS P WHERE P.index = 20) AS GENESYS_ID,
       IFNULL((SELECT P.value FROM UNNEST(ei.customDimensions) AS P WHERE P.index = 98), "-") AS INDICE,
       ei.hitNumber AS Numero
FROM `dbi-santanderrio-2324477.137275638.ga_sessions_%1`,
UNNEST(hits) AS ei
WHERE ei.eventInfo.eventCategory IN ("Watson","Chat")
AND ei.eventInfo.eventAction IN ("Pregunta","Respuesta")
ORDER BY CONVERSACION_ID ASC, TIMESTAMP ASC, TEXTO_INTERACCION DESC
)
select
--a.fullvisitorid,
a.CHANNEL,
CASE
  WHEN a.CONVERSACION_ID IS NULL AND a.GENESYS_ID IS NULL THEN "NotSet"
  WHEN a.GENESYS_ID IS NULL AND a.CONVERSACION_ID IS NOT NULL THEN a.CONVERSACION_ID
  WHEN a.GENESYS_ID IS NOT NULL AND a.CONVERSACION_ID IS NULL THEN a.GENESYS_ID
  ELSE CONCAT(a.CONVERSACION_ID, "-", a.GENESYS_ID)
END AS CHAT_ID,
a.TIPO_INTERACCION,
CONCAT(a.TEXTO_INTERACCION, "-", a.INDICE) AS TEXTO_INTERACCION,
a.TIMESTAMP,
a.TIPO_RESPUESTA,
a.INTENCION,
a.CONFIANZA,
b.SATISFACCION,
CASE WHEN LOWER(a.CHANNEL) = "portal" THEN
  CASE WHEN b.PTOS_SATISFACCION >= "1" AND b.PTOS_SATISFACCION <= "5" THEN
    b.PTOS_SATISFACCION
  ELSE
    "Cerrar sin calificar"
  END
ELSE
  b.PTOS_SATISFACCION
END AS PTOS_SATISFACCION,
a.PANTALLA_APERTURA
FROM tabla a
LEFT JOIN (
SELECT
       ei.eventInfo.eventAction AS SATISFACCION, --CustomDimensions
       REPLACE(ei.eventInfo.eventLabel,",",".") AS PTOS_SATISFACCION, --CustomDimensions
       cd.value AS CONVERSACION_ID,
       fullvisitorid,
       clientid
FROM `dbi-santanderrio-2324477.137275638.ga_sessions_%1`,
UNNEST(hits) AS ei,
UNNEST(ei.customDimensions) AS cd
WHERE ei.eventInfo.eventCategory IN ("Watson","Chat")
AND LOWER(ei.eventInfo.eventAction) IN ("satisfaccion", "confirmacion respuesta valida")
AND cd.index = 11
AND cd.value IS NOT NULL
group by ei.eventInfo.eventAction, ei.eventInfo.eventLabel, cd.value, fullvisitorid, clientid
) b
ON a.CONVERSACION_ID = b.CONVERSACION_ID
AND a.clientId = b.clientid
ORDER BY CHANNEL ASC, CHAT_ID ASC, TIMESTAMP ASC, TEXTO_INTERACCION ASC