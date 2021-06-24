SELECT feoperac, s1emp, contra1, feccali, tipmodel, tipmode2, idmodel, tipo, c1digcon FROM(
	SELECT 0 AS ORDEN,
		'feoperac' as feoperac, 's1emp' as s1emp,
		'contra1' as contra1, 'feccali' as feccali,
		'tipmodel' as tipmodel, 'tipmode2' as tipmode2,
		'idmodel' as idmodel, 'tipo' as tipo,
		'c1digcon' as c1digcon
	UNION ALL
	SELECT DISTINCT 1 AS ORDEN,
		trim(e9952_feoperac) as feoperac,
		trim(e9952_s1emp) as s1emp,
		trim(e9952_contra1) as contra1,
		trim(e9952_feccali) as feccali,
		trim(e9952_tipmodel) as tipmodel,
		trim(e9952_tipmode2) as tipmode2,
		trim(e9952_idmodel) as idmodel,
		trim(e9952_tipo) as tipo,
		trim(e9952_c1digcon) as c1digcon
	FROM bi_corp_bdr.jm_cal_in_co
WHERE partition_date = '$month_partition_bdr') A
ORDER BY ORDEN ASC;