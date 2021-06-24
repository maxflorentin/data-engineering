WITH 
	DIMENSIONES AS (
	SELECT
		fullVisitorId,
		visitId,
		cd.Index AS Index,
		cd.Value AS Value,
		H.hitNumber AS Hit_Number,
		CAST(FORMAT_TIMESTAMP("%d/%m/%Y %H:%M:%S",
					TIMESTAMP_SECONDS(CAST(visitStartTime + H.time/1000 AS INT64)),
					"America/Argentina/Buenos_Aires") AS STRING) AS Hit_Timestamp
	FROM `137275638.ga_sessions_{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date_filter', dag_id='DOWNLOAD_GA-Daily') }}`,
	UNNEST(hits) AS H,
	UNNEST(H.customDimensions) as cd
	WHERE H.dataSource = "web"
	AND (
		REGEXP_CONTAINS(H.page.pagePath, 'www2.personas.santander.com.ar/online-banking-personas/') OR
		H.page.hostname IN ("www2.canjedepuntos.santander.com.ar", "www.segurossantanderrio.com.ar") 
	  ) AND (
		NOT REGEXP_CONTAINS(H.page.pagePath, 'www.personas.santander.com.ar/online-banking/login') AND
		NOT REGEXP_CONTAINS(H.page.pagePath, 'www2.personas.santander.com.ar/online-banking-personas/000/')
	  )
	AND ((cd.index >= 8 AND cd.index <= 14) OR (cd.index > 20))
	GROUP BY 1,2,3,4,5,6
	)
,
	deduplicate_dimensions AS (
	SELECT
		FullVisitorID,
		visitid,
		Hit_Number,
		Hit_Timestamp,
		(SELECT AS STRUCT Index, Value) AS CD
	FROM DIMENSIONES
	)
,
	struct_dimensions AS (
	Select
		FullVisitorID AS Full_Visitor_ID,
		visitid AS Session_ID,
		Hit_Number,
		Hit_Timestamp,
		TO_JSON_STRING(ARRAY_AGG(CD)) as CD
	 from deduplicate_dimensions
	 group by 1,2,3,4
	)
, 
metrics AS (
	SELECT
		fullVisitorId,
		visitId,
		cm.Index AS Index,
		cm.Value AS Value,
		H.hitNumber AS Hit_Number,
		CAST(FORMAT_TIMESTAMP("%d/%m/%Y %H:%M:%S",
					TIMESTAMP_SECONDS(CAST(visitStartTime + H.time/1000 AS INT64)),
					"America/Argentina/Buenos_Aires") AS STRING) AS Hit_Timestamp
	FROM `137275638.ga_sessions_{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date_filter', dag_id='DOWNLOAD_GA-Daily') }}`,
	UNNEST(hits) AS H,
	UNNEST(H.customMetrics) as cm
	WHERE H.dataSource = "web"
	AND (
		REGEXP_CONTAINS(H.page.pagePath, 'www2.personas.santander.com.ar/online-banking-personas/') OR
		H.page.hostname IN ("www2.canjedepuntos.santander.com.ar", "www.segurossantanderrio.com.ar") 
	  ) AND (
		NOT REGEXP_CONTAINS(H.page.pagePath, 'www.personas.santander.com.ar/online-banking/login') AND
		NOT REGEXP_CONTAINS(H.page.pagePath, 'www2.personas.santander.com.ar/online-banking-personas/000/')
	  )
	GROUP BY 1,2,3,4,5,6
	)
,
	deduplicate_metrics AS (
	SELECT
		FullVisitorID,
		visitid,
		Hit_Number,
		Hit_Timestamp,
		(SELECT AS STRUCT Index, Value) AS CM
	FROM metrics
	)
,
	struct_metrics AS (
	Select
		FullVisitorID AS Full_Visitor_ID,
		visitid AS Session_ID,
		Hit_Number,
		Hit_Timestamp,
		TO_JSON_STRING(ARRAY_AGG(CM)) AS CM
	from deduplicate_metrics
	group by 1,2,3,4
	)
,
	tbl_base AS (
SELECT
fullVisitorId AS Full_Visitor_ID,

(SELECT MAX(IF (custom.index = 15, custom.value,  NULL)) FROM UNNEST(H.customDimensions) AS custom) AS GA_Client_ID,
(SELECT MAX(IF (custom.index = 2, regexp_replace(custom.value, "[^a-zA-Z ]", ''),  NULL)) FROM UNNEST(H.customDimensions) AS custom) AS Tipo_de_Cliente,
IFNULL((SELECT MAX(IF (custom.index = 1, custom.value,  NULL)) FROM UNNEST(H.customDimensions) AS custom), 'not set') AS NUP,
(SELECT MAX(IF (custom.index = 4, regexp_replace(custom.value, "[^a-zA-Z ]", ''),  NULL)) FROM UNNEST(H.customDimensions) AS custom) AS Segmento_de_Cliente,

visitNumber AS Session_Number,
visitId AS Session_ID,
CAST(FORMAT_TIMESTAMP("%d/%m/%Y %H:%M:%S",
            TIMESTAMP_SECONDS(visitStartTime),
            "America/Argentina/Buenos_Aires") AS STRING) AS Session_StartTime,
totals.timeOnSite AS Session_Duration,
totals.hits AS Session_Hits,
totals.pageviews AS Session_Pageviews,
totals.bounces AS Session_Bounces,
totals.transactions AS Session_transactions,
totals.visits AS Session_with_events,
totals.newVisits AS Session_with_new_user,

(SELECT MAX(IF (custom.index = 16, regexp_replace(custom.value, "[^a-zA-Z ]", ''),  NULL)) FROM UNNEST(H.customDimensions) AS custom) AS Tipo_de_Identificacion,
(SELECT MAX(IF (custom.index = 3, regexp_replace(custom.value, "[^a-zA-Z ]", ''),  NULL)) FROM UNNEST(H.customDimensions) AS custom) AS Login_status,

channelGrouping AS TrafficSource_Channel,
trafficSource.referralPath AS TrafficSource_Referral,
trafficSource.campaign AS TrafficSource_Campaign,
trafficSource.source AS TrafficSource_Source,
trafficSource.medium AS TrafficSource_Medium,
SUBSTR(regexp_replace(trafficSource.keyword, "[^a-zA-Z ]", ''), 1, 254) AS TrafficSource_Keyword,
trafficSource.adContent AS TrafficSource_AdContent,

device.browser AS device_browser,
device.browserVersion AS device_browserVersion,
device.browserSize AS device_browserSize,
device.operatingSystem AS device_operatingSystem,
device.operatingSystemVersion AS device_operatingSystemVersion,
device.mobileDeviceBranding AS device_mobileDeviceBranding,
device.mobileDeviceModel AS device_mobileDeviceModel,
device.mobileInputSelector AS device_mobileInputSelector,
device.mobileDeviceInfo AS device_mobileDeviceInfo,
device.javaEnabled AS device_javaEnabled,
device.language AS device_language,
device.screenColors AS device_screenColors,
device.screenResolution AS device_screenResolution,
device.deviceCategory AS device_deviceCategory,

geoNetwork.country AS User_Country,
geoNetwork.city AS User_City,
geoNetwork.networkDomain AS User_Network,
geoNetwork.networkLocation AS User_Network_Location,

H.hitNumber AS Hit_Number,
H.type AS Hit_Type,
CAST(FORMAT_TIMESTAMP("%d/%m/%Y %H:%M:%S",
            TIMESTAMP_SECONDS(CAST(visitStartTime + H.time/1000 AS INT64)),
            "America/Argentina/Buenos_Aires") AS STRING) AS Hit_Timestamp,
H.isEntrance AS Hit_Entrance,
H.isExit AS Hit_Exit,
SUBSTR(H.referer,1,254) AS Hit_Referer,

MAX(H.contentGroup.contentGroup1) OVER (PARTITION BY fullVisitorId) AS Content_Group_1,
MAX(H.contentGroup.contentGroup2) OVER(PARTITION BY fullVisitorId) AS Content_Group_2,
MAX(H.contentGroup.contentGroup3) OVER(PARTITION BY fullVisitorId) AS Content_Group_3,

H.page.hostname AS Hostname,
H.page.pagePath AS Screen,
REPLACE(REGEXP_REPLACE(H.page.pageTitle, "[^a-zA-Z ]", ''),'|',';') AS Uri,

REPLACE(REGEXP_REPLACE(H.eventInfo.eventCategory, "[^a-zA-Z ]", ''),'|',';') AS Event_Category,
REPLACE(SUBSTR(REGEXP_REPLACE(H.eventInfo.eventAction, "[^a-zA-Z ]", ''), 1, 254),'|',';') AS Event_Action,
REPLACE(SUBSTR(REGEXP_REPLACE(H.eventInfo.eventLabel, "[^a-zA-Z ]", ''),1,254),'|',';') AS Event_Label,
H.eventInfo.eventValue AS Event_Value,
(SELECT MAX(promoId) FROM UNNEST(H.promotion)) AS Promo_Id,
(SELECT MAX(promoName) FROM UNNEST(H.promotion)) AS Promo_Name,
(SELECT MAX(promoCreative) FROM UNNEST(H.promotion)) AS Promo_Creative,
(SELECT MAX(promoPosition) FROM UNNEST(H.promotion)) AS Promo_Position,
REGEXP_REPLACE(H.page.searchKeyword, "[^a-zA-Z ]", '') AS Search_Keyword,
REGEXP_REPLACE(H.page.searchCategory, "[^a-zA-Z ]", '') AS Search_Category,

(SELECT MAX(IF (metrics.index = 9, metrics.value,  NULL)) FROM UNNEST (H.custommetrics) AS metrics) AS Watson_Tipo,
(SELECT MAX(IF (metrics.index = 10, metrics.value,  NULL)) FROM UNNEST (H.custommetrics) AS metrics) AS Watson_Intencion,
(SELECT MAX(IF (metrics.index = 11, metrics.value,  NULL)) FROM UNNEST (H.custommetrics) AS metrics) AS Watson_Conversation_ID,
(SELECT MAX(IF (metrics.index = 1, metrics.value,  NULL)) FROM UNNEST (H.custommetrics) AS metrics) AS Watson_Confianza,
(SELECT MAX(IF (metrics.index = 2, metrics.value,  NULL)) FROM UNNEST (H.custommetrics) AS metrics) AS Watson_Duracion,
(SELECT MAX(IF (custom.index = 18, custom.value,  NULL)) FROM UNNEST(H.customDimensions) AS custom) AS Canal,
geoNetwork.latitude AS Coordenadas_latitude,
geoNetwork.longitude AS Coordenadas_longitude,
null tealium_visitor_id,
REPLACE(H.page.pageTitle,'|',';') AS page_tittle
FROM `137275638.ga_sessions_{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date_filter', dag_id='DOWNLOAD_GA-Daily') }}`,
UNNEST(hits) AS H
WHERE H.dataSource = "web"
AND (NOT H.page.hostname = "www.personas.santanderrio.com.ar"
AND NOT H.page.hostname = "www2.personas.santanderrio.com.ar"
AND NOT H.page.hostname = "www.segurossantanderrio.com.ar"
AND NOT H.page.hostname = "www2.canjedepuntos.santanderrio.com.ar"
AND NOT H.page.hostname = "www.emp.santanderrio.com.ar")
OR (H.page.hostname = "www2.canjedepuntos.santanderrio.com.ar" OR H.page.hostname = "www.segurossantanderrio.com.ar")
)
	
SELECT tbl_base.*, 
	CD.CD AS Custom_Dimensions,
	CM.CM AS Custom_Metrics 
FROM tbl_base
LEFT JOIN struct_dimensions CD 
USING(Full_Visitor_ID, Session_ID, Hit_Number, Hit_Timestamp)
LEFT JOIN struct_metrics CM
USING(Full_Visitor_ID, Session_ID, Hit_Number, Hit_Timestamp)