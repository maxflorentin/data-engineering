SELECT
fullVisitorId AS Full_Visitor_ID,

(SELECT MAX(IF (custom.index = 15, custom.value,  NULL)) FROM UNNEST(H.customDimensions) AS custom) AS GA_Client_ID,
(SELECT MAX(IF (custom.index = 2, custom.value,  NULL)) FROM UNNEST(H.customDimensions) AS custom) AS Tipo_de_Cliente,
IFNULL((SELECT MAX(IF (custom.index = 1, custom.value,  NULL)) FROM UNNEST(H.customDimensions) AS custom), 'not set') AS NUP,
(SELECT MAX(IF (custom.index = 4, custom.value,  NULL)) FROM UNNEST(H.customDimensions) AS custom) AS Segmento_de_Cliente,

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

(SELECT MAX(IF (custom.index = 16, custom.value,  NULL)) FROM UNNEST(H.customDimensions) AS custom) AS Tipo_de_Identificacion,
(SELECT MAX(IF (custom.index = 3, custom.value,  NULL)) FROM UNNEST(H.customDimensions) AS custom) AS Login_status,

channelGrouping AS TrafficSource_Channel,
trafficSource.referralPath AS TrafficSource_Referral,
trafficSource.campaign AS TrafficSource_Campaign,
trafficSource.source AS TrafficSource_Source,
trafficSource.medium AS TrafficSource_Medium,
SUBSTR(trafficSource.keyword, 1, 254) AS TrafficSource_Keyword,
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
REPLACE(H.page.pageTitle,'|',';') AS Uri,

REPLACE(H.eventInfo.eventCategory,'|',';') AS Event_Category,
REPLACE(SUBSTR(H.eventInfo.eventAction, 1, 254),'|',';') AS Event_Action,
REPLACE(SUBSTR(H.eventInfo.eventLabel,1,254),'|',';') AS Event_Label,
H.eventInfo.eventValue AS Event_Value,
(SELECT MAX(promoId) FROM UNNEST(H.promotion)) AS Promo_Id,
(SELECT MAX(promoName) FROM UNNEST(H.promotion)) AS Promo_Name,
(SELECT MAX(promoCreative) FROM UNNEST(H.promotion)) AS Promo_Creative,
(SELECT MAX(promoPosition) FROM UNNEST(H.promotion)) AS Promo_Position,
H.page.searchKeyword AS Search_Keyword,
H.page.searchCategory AS Search_Category,

(SELECT MAX(IF (metrics.index = 9, metrics.value,  NULL)) FROM UNNEST (H.custommetrics) AS metrics) AS Watson_Tipo,
(SELECT MAX(IF (metrics.index = 10, metrics.value,  NULL)) FROM UNNEST (H.custommetrics) AS metrics) AS Watson_Intencion,
(SELECT MAX(IF (metrics.index = 11, metrics.value,  NULL)) FROM UNNEST (H.custommetrics) AS metrics) AS Watson_Conversation_ID,
(SELECT MAX(IF (metrics.index = 1, metrics.value,  NULL)) FROM UNNEST (H.custommetrics) AS metrics) AS Watson_Confianza,
(SELECT MAX(IF (metrics.index = 2, metrics.value,  NULL)) FROM UNNEST (H.custommetrics) AS metrics) AS Watson_Duracion,
(SELECT MAX(IF (custom.index = 18, custom.value,  NULL)) FROM UNNEST(H.customDimensions) AS custom) AS Canal,
geoNetwork.latitude AS Coordenadas_latitude,
geoNetwork.longitude AS Coordenadas_longitude
FROM `dbi-santanderrio-2324477.137275638.ga_sessions_20191024`,
UNNEST(hits) AS H
WHERE H.dataSource = "web"
AND (NOT H.page.hostname = "www.personas.santanderrio.com.ar"
AND NOT H.page.hostname = "www2.personas.santanderrio.com.ar"
AND NOT H.page.hostname = "www.segurossantanderrio.com.ar"
AND NOT H.page.hostname = "www2.canjedepuntos.santanderrio.com.ar"
AND NOT H.page.hostname = "www.emp.santanderrio.com.ar")
OR (H.page.hostname = "www2.canjedepuntos.santanderrio.com.ar" OR H.page.hostname = "www.segurossantanderrio.com.ar")
limit 1000000
