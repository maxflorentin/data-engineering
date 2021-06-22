
--     ___|         |                   |              
--    |       _` |  |   _ \  __ \    _` |   _` |   __| 
--    |      (   |  |   __/  |   |  (   |  (   |  |    
--   \____| \__,_| _| \___| _|  _| \__,_| \__,_| _|    
-- 
--                                         
CREATE TEMPORARY TABLE calendar AS 
WITH 
dates AS (
SELECT
	date_add("1980-01-01", a.pos) dt_tie_date FROM (SELECT posexplode(split(repeat(",", 25932),","))) a )
, dates_expanded AS (
SELECT da.dt_tie_date 
	, add_months(da.dt_tie_date,-12) dt_tie_datepreviousyear
	, add_months(da.dt_tie_date,-1) dt_tie_datepreviousmonth
	, date_add(da.dt_tie_date, -1) dt_tie_datepreviousdate
	, date_add(da.dt_tie_date, -7) dt_tie_datepreviousweek
	, date_format(da.dt_tie_date, 'YYYY-MM-01') dt_tie_firstdateofmonth	
	, last_day(da.dt_tie_date) dt_tie_lastdateofmonth
	-- ------------------------------------------------------------------
	, date_format(da.dt_tie_date, 'w') int_tie_weekofyear
	, date_format(add_months(da.dt_tie_date,-1), 'w') int_tie_previousmonthweekofyear
	, date_format(add_months(da.dt_tie_date,-12), 'w') int_tie_previousyearweekofyear
	, MONTH(da.dt_tie_date) int_tie_month
	, date_format(da.dt_tie_date, 'YYYY-MM') ds_tie_yearmonth
	, date_format(add_months(da.dt_tie_date,-1), 'YYYY-MM') ds_tie_previousmonthyearmonth
	, date_format(add_months(da.dt_tie_date,-12), 'YYYY-MM') ds_tie_previousyearyearmonth
	, YEAR(da.dt_tie_date) int_tie_year
	, YEAR(da.dt_tie_date)-1 int_tie_previousyear
	, date_format(da.dt_tie_date,'u') int_tie_dayofweek
	, date_format(da.dt_tie_date, 'W') int_tie_weekofmonth
	, CAST(MONTH(da.dt_tie_date)/ 4 + 1 AS BIGINT) int_tie_quarter
	, CAST(MONTH(da.dt_tie_date)/ 3 + 1 AS BIGINT) int_tie_trimester
	, CAST(MONTH(da.dt_tie_date)/ 6 + 1 AS BIGINT) int_tie_semester
	-- ------------------------------------------------------------------
	, date_format(dt_tie_date, 'EEEE') ds_tie_day 
	, date_format(da.dt_tie_date,'MMMMM') ds_tie_month
	, IF(fe.`200_nbfhfest` IS NOT NULL AND TRIM(fe.`200_nbfhfest`) != 'SABADO', fe.`200_nbfhfest`, NULL) ds_tie_holiday  
	-- ------------------------------------------------------------------
	, IF(date_format(da.dt_tie_date,'u') IN (6,7), 1, 0) flag_tie_weekend 
	, IF((fe.`200_fhfestiv` IS NOT NULL AND fe.200_tpfestiv IN (1,4)) OR date_format(da.dt_tie_date,'u') IN (6,7), 1, 0) flag_tie_holiday 
	, IF(fe.`200_fhfestiv` IS NULL AND date_format(da.dt_tie_date,'u') NOT IN (6,7), 1, 0) flag_tie_workingday
	, fe.partition_date
FROM dates da
LEFT JOIN bi_corp_staging.maltc_tcdv0200 fe ON
	fe.partition_date = '2020-11-18'
	AND fe.200_tpfestiv IN (1,4)
	AND fe.200_fhfestiv = da.dt_tie_date )

SELECT * FROM dates_expanded a ;

-- __/__/__/__/__/__/__/__/__/__/__/__/__/__/__/__/__/__/__/__/__/__/__/ LOAD TABLE

set hive.execution.engine=spark;
set spark.yarn.queue=root.dataeng;
set hive.exec.dynamic.partition.mode=nonstrict;

INSERT OVERWRITE TABLE bi_corp_common.dim_tie_calendario
PARTITION(partition_date)
SELECT ca.dt_tie_date
	, ca.dt_tie_datepreviousyear
	, ca.dt_tie_datepreviousmonth
	, ca.dt_tie_datepreviousdate
	, ca.dt_tie_datepreviousweek
	, COALESCE(nw1.dt_tie_date,nw2.dt_tie_date,nw3.dt_tie_date,nw4.dt_tie_date,nw5.dt_tie_date) dt_tie_nextworkingdate
	, COALESCE(lw1.dt_tie_date,lw2.dt_tie_date,lw3.dt_tie_date,lw4.dt_tie_date,lw5.dt_tie_date) dt_tie_previousworkingdate
	, MIN(wm.dt_tie_date) OVER (PARTITION BY ca.ds_tie_yearmonth) dt_tie_firstworkingdate
	, MAX(wm.dt_tie_date) OVER (PARTITION BY ca.ds_tie_yearmonth) dt_tie_lastworkingdate
	, ca.dt_tie_firstdateofmonth
	, ca.dt_tie_lastdateofmonth
	, ca.int_tie_weekofyear
	, ca.int_tie_previousmonthweekofyear
	, ca.int_tie_previousyearweekofyear
	, ca.int_tie_month
	, ca.ds_tie_yearmonth
	, ca.ds_tie_previousmonthyearmonth
	, ca.ds_tie_previousyearyearmonth
	, ca.int_tie_year
	, ca.int_tie_previousyear
	, ca.int_tie_dayofweek
	, ca.int_tie_weekofmonth
	, ca.int_tie_quarter
	, ca.int_tie_trimester
	, ca.int_tie_semester
	, ca.ds_tie_day
	, ca.ds_tie_month
	, ca.ds_tie_holiday
	, ca.flag_tie_weekend
	, ca.flag_tie_holiday
	, ca.flag_tie_workingday
	, ca.partition_date
FROM calendar ca
LEFT JOIN calendar wm 
	ON ca.dt_tie_date = wm.dt_tie_date
	AND wm.flag_tie_workingday = 1
-- previous worwing date
LEFT JOIN calendar lw1 
	ON ca.dt_tie_datepreviousdate = lw1.dt_tie_date
	AND lw1.flag_tie_workingday = 1
LEFT JOIN calendar lw2 
	ON date_add(ca.dt_tie_datepreviousdate, -1) = lw2.dt_tie_date
	AND lw2.flag_tie_workingday = 1
LEFT JOIN calendar lw3 
	ON date_add(ca.dt_tie_datepreviousdate, -2) = lw3.dt_tie_date
	AND lw3.flag_tie_workingday = 1
LEFT JOIN calendar lw4 
	ON date_add(ca.dt_tie_datepreviousdate, -3) = lw4.dt_tie_date
	AND lw4.flag_tie_workingday = 1
LEFT JOIN calendar lw5 
	ON date_add(ca.dt_tie_datepreviousdate, -4) = lw5.dt_tie_date
	AND lw5.flag_tie_workingday = 1
-- next working date
LEFT JOIN calendar nw1 
	ON date_add(ca.dt_tie_date, 1) = nw1.dt_tie_date
	AND nw1.flag_tie_workingday = 1
LEFT JOIN calendar nw2 
	ON date_add(ca.dt_tie_date, 2) = nw2.dt_tie_date
	AND nw2.flag_tie_workingday = 1
LEFT JOIN calendar nw3 
	ON date_add(ca.dt_tie_date, 3) = nw3.dt_tie_date
	AND lw3.flag_tie_workingday = 1
LEFT JOIN calendar nw4 
	ON date_add(ca.dt_tie_date, 4) = nw4.dt_tie_date
	AND lw4.flag_tie_workingday = 1
LEFT JOIN calendar nw5 
	ON date_add(ca.dt_tie_date, 5) = nw5.dt_tie_date
	AND nw5.flag_tie_workingday = 1 ;