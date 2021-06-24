SELECT feoperac, s1emp, contra1, id_prod, idpro_lc,
       fechaper, ind_titu, id_natur, idprolc2 FROM(
	SELECT 0 AS ORDEN ,
		'feoperac' as feoperac, 's1emp'    as s1emp,
		'contra1'  as contra1, 'id_prod'   as id_prod,
		'idpro_lc' as idpro_lc, 'fechaper' as fechaper,
		'ind_titu' as ind_titu, 'id_natur' as id_natur,
		'idprolc2' as idprolc2
	UNION ALL
	SELECT DISTINCT 1 AS ORDEN,
		trim(g4095_feoperac) as feoperac,
		trim(g4095_s1emp)    as s1emp,
		trim(g4095_contra1)  as contra1,
		trim(g4095_id_prod)  as id_prod,
		trim(g4095_idpro_lc) as idpro_lc,
		trim(g4095_fechaper) as fechaper,
		trim(g4095_ind_titu) as ind_titu,
		trim(g4095_id_natur) as id_natur,
		trim(g4095_idprolc2) as idprolc2
	FROM bi_corp_bdr.jm_contr_bis_bkup
	WHERE partition_date = '$month_partition_bdr') A
ORDER BY ORDEN ASC;