WITH DIMENSIONES AS (
SELECT
  fullVisitorId,
  visitId,
  cd.Index AS Index,
  cd.Value AS Value,
  H.hitNumber AS Hit_Number,
  CAST(FORMAT_TIMESTAMP("%d/%m/%Y %H:%M:%S",
            TIMESTAMP_SECONDS(CAST(visitStartTime + H.time/1000 AS INT64)),
            "America/Argentina/Buenos_Aires") AS STRING) AS Hit_Timestamp
FROM `dbi-santanderrio-2324477.137275638.ga_sessions_%1`,
UNNEST(hits) AS H,
UNNEST(H.customDimensions) as cd
WHERE H.dataSource = "app"
AND H.page.hostname IS NULL 
AND ((cd.index >= 8 AND cd.index <= 14) OR (cd.index > 20))
GROUP BY 1,2,3,4,5,6
),
 deduplicate_dimensions AS (
 SELECT
 FullVisitorID,
 visitid,
 Hit_Number,
 Hit_Timestamp,
 (SELECT AS STRUCT Index, Value) AS CD
  FROM DIMENSIONES
 ),
 struct_dimensions AS (
Select
    FullVisitorID AS Full_Visitor_ID,
    visitid AS Session_ID,
    Hit_Number,
    Hit_Timestamp,
    TO_JSON_STRING(ARRAY_AGG(CD)) as CD
 from deduplicate_dimensions
 group by 1,2,3,4
), metrics AS (
SELECT
  fullVisitorId,
  visitId,
  cm.Index AS Index,
  cm.Value AS Value,
  H.hitNumber AS Hit_Number,
  CAST(FORMAT_TIMESTAMP("%d/%m/%Y %H:%M:%S",
            TIMESTAMP_SECONDS(CAST(visitStartTime + H.time/1000 AS INT64)),
            "America/Argentina/Buenos_Aires") AS STRING) AS Hit_Timestamp
FROM `dbi-santanderrio-2324477.137275638.ga_sessions_%1`,
UNNEST(hits) AS H,
UNNEST(H.customMetrics) as cm
WHERE H.dataSource = "app"
AND H.page.hostname IS NULL 
GROUP BY 1,2,3,4,5,6
),
 deduplicate_metrics AS (
 SELECT
 FullVisitorID,
 visitid,
 Hit_Number,
 Hit_Timestamp,
 (SELECT AS STRUCT Index, Value) AS CM
  FROM metrics
 ),
 struct_metrics AS (
Select
    FullVisitorID AS Full_Visitor_ID,
    visitid AS Session_ID,
    Hit_Number,
    Hit_Timestamp,
    TO_JSON_STRING(ARRAY_AGG(CM)) AS CM
 from deduplicate_metrics
 group by 1,2,3,4
),
tbl_base AS (
SELECT
  fullVisitorId AS Full_Visitor_ID,
  null AS Tealium_Visitor_ID,
  (SELECT (MAX(IF (custom.index = 15, custom.value,  NULL))) FROM UNNEST(H.customDimensions) AS custom) AS GA_Client_ID,
  (SELECT MAX(IF (custom.index = 2, custom.value,  NULL)) FROM UNNEST(H.customDimensions) AS custom) AS Client_Type,
  IFNULL((SELECT MAX(IF (custom.index = 1, custom.value, NULL)) FROM UNNEST(H.customDimensions) AS custom), 'not set') AS NUP,
  (SELECT (MAX(IF (custom.index = 4, custom.value,  NULL))) FROM UNNEST(H.customDimensions) AS custom) AS Client_Segment,
  geoNetwork.country AS User_Country,
  geoNetwork.city AS User_City,
  geoNetwork.networkDomain AS User_Network,
  geoNetwork.networkLocation AS User_Network_Location,
  geoNetwork.latitude AS latitude,
  geoNetwork.longitude AS longitude,

  visitNumber AS Session_Number,
  visitId AS Session_ID,
  CAST(FORMAT_TIMESTAMP("%d/%m/%Y %H:%M:%S",
              TIMESTAMP_SECONDS(visitStartTime),
              "America/Argentina/Buenos_Aires") AS STRING) AS Session_StartTime,
  totals.timeOnSite AS Session_Duration,
  totals.hits AS Session_Hits,
  totals.bounces AS Session_Bounces,
  totals.newVisits AS Session_with_new_user,
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
  H.hitNumber AS Hit_Number,
  CAST(FORMAT_TIMESTAMP("%d/%m/%Y %H:%M:%S",
              TIMESTAMP_SECONDS(CAST(visitStartTime + H.time/1000 AS INT64)),
              "America/Argentina/Buenos_Aires") AS STRING) AS Hit_Timestamp,
  H.type AS Hit_Type,
  H.isEntrance AS Hit_Entrance,
  H.isExit AS Hit_Exit,
  H.appInfo.screenName AS Screen,
  (SELECT MAX(IF (custom.index = 18, custom.value,  NULL)) FROM UNNEST(H.customDimensions) AS custom) AS Channel,
  REPLACE(H.eventInfo.eventCategory,'|',';') AS Event_Category,
  REPLACE(SUBSTR(H.eventInfo.eventAction, 1, 254),'|',';') AS Event_Action,
  REPLACE(SUBSTR(H.eventInfo.eventLabel,1,254),'|',';') AS Event_Label
FROM `dbi-santanderrio-2324477.137275638.ga_sessions_%1`,
  UNNEST(hits) AS H
  WHERE H.dataSource = "app"
  AND H.page.hostname IS NULL 
)
SELECT tbl_base.*, 
CD.CD AS Custom_Dimensions,
CM.CM AS Custom_Metrics 
FROM tbl_base
LEFT JOIN struct_dimensions CD 
USING(Full_Visitor_ID, Session_ID, Hit_Number, Hit_Timestamp)
LEFT JOIN struct_metrics CM
USING(Full_Visitor_ID, Session_ID, Hit_Number, Hit_Timestamp)