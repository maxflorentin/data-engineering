SELECT feoperac, s1emp, contra1 FROM(
	SELECT 0 AS ORDEN ,
		'feoperac' as feoperac, 's1emp' as s1emp, 'contra1' as contra1
	UNION ALL
	SELECT DISTINCT 1 AS ORDEN,
		trim(h0711_feoperac) as feoperac,
		trim(h0711_s1emp) as s1emp,
		trim(h0711_contra1) as contra1
	FROM bi_corp_bdr.jm_ctos_cance_bkup
	WHERE partition_date = '$month_partition_bdr') A
ORDER BY ORDEN ASC;