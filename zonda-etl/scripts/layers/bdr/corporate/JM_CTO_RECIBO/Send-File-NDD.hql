SELECT feoperac, s1emp, contra1, idrecibo,
	   fec_orig, imp_rdev, fec_dev FROM(
	SELECT 0 AS ORDEN,
		'feoperac' as feoperac, 's1emp' as s1emp,
		'contra1' as contra1, 'idrecibo' as idrecibo,
		'fec_orig' as fec_orig, 'imp_rdev' as imp_rdev,
		'fec_dev' as fec_dev
	UNION ALL
	SELECT DISTINCT 1 AS ORDEN,
		trim(t1754_feoperac) as feoperac,
		trim(t1754_s1emp) as s1emp,
		trim(t1754_contra1) as contra1,
		trim(t1754_idrecibo) as idrecibo,
		trim(t1754_fec_orig) as fec_orig, 
		concat(substring(trim(t1754_imp_rdev),1,15),'.',substring(trim(t1754_imp_rdev),17,2)) as imp_rdev,
		trim(t1754_fec_dev) as fec_dev
	FROM bi_corp_bdr.jm_cto_recibo
WHERE partition_date  = '$last_calendar_day') A
ORDER BY ORDEN ASC;